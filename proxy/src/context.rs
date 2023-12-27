//! Connection request contexts

use std::{io, net::IpAddr};

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use parquet::{
    column::writer::{ColumnCloseResult, ColumnWriterImpl},
    data_type::{ByteArrayType, FixedLenByteArrayType},
    file::{
        properties::WriterPropertiesPtr,
        writer::{SerializedFileWriter, SerializedPageWriter, TrackedWrite},
    },
    format::FileMetaData,
    schema::types::{SchemaDescPtr, TypePtr},
};
use serde::{Serialize, Serializer};
use smol_str::SmolStr;
use tokio::sync::mpsc::Receiver;
use tokio_util::task::TaskTracker;
use tracing::info;
use uuid::Uuid;

use crate::{error::ErrorKind, metrics::LatencyTimer};

#[derive(Clone, serde::Serialize)]
/// Context data for a single request to connect to a database.
///
/// This data should **not** be used for connection logic, only for observability and limiting purposes.
/// All connection logic should instead use strongly typed state machines, not a bunch of Options.
pub struct RequestContext {
    pub peer_addr: IpAddr,
    pub session_id: Uuid,
    #[serde(skip)]
    pub first_packet: chrono::DateTime<Utc>,
    pub protocol: &'static str,
    pub project: Option<SmolStr>,
    pub branch: Option<SmolStr>,
    pub endpoint_id: Option<SmolStr>,
    pub user: Option<SmolStr>,
    pub application: Option<SmolStr>,
    pub region: &'static str,
    pub error_kind: Option<ErrorKind>,
    #[serde(rename = "request_latency", serialize_with = "latency_timer")]
    pub latency_timer: LatencyTimer,
    #[serde(skip)]
    logged: bool,
}

fn latency_timer<S: Serializer>(timer: &LatencyTimer, s: S) -> Result<S::Ok, S::Error> {
    timer.accumulated.serialize(s)
}

impl RequestContext {
    pub fn new(
        session_id: Uuid,
        peer_addr: IpAddr,
        protocol: &'static str,
        region: &'static str,
    ) -> Self {
        Self {
            peer_addr,
            session_id,
            protocol,
            latency_timer: LatencyTimer::new(protocol),
            first_packet: Utc::now(),
            project: None,
            branch: None,
            endpoint_id: None,
            user: None,
            application: None,
            region,
            error_kind: None,
            logged: false,
        }
    }

    pub fn log(&mut self) {
        if !self.logged {
            self.logged = true;
            info!("{}", serde_json::to_string(self).unwrap());
        }
    }
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        self.log()
    }
}

// the parquet crate leaves a lot to be desired...
// what follows is an attempt to write parquet files with minimal allocs.
// complication: parquet is a columnar format, while we want to write in as rows.
// design:
// * we have a fixed number of columns, so we can represent the file with N column writers
// * when we have written enough data, we can flush the columns into a single buffer to be uploaded
//
// We might be able to skip the second buffer and stream it directly to S3. Will need faff because no async API.

pub struct ParquetStreamOwned {
    properties: WriterPropertiesPtr,
    schema: SchemaDescPtr,
    session_id_stream: TrackedWrite<BytesWriter>,
    peer_addr_stream: TrackedWrite<BytesWriter>,
}

/// Column writers for each column we have
pub struct ParquetStreamWriter<'a> {
    session_id_stream: ColumnWriterImpl<'a, FixedLenByteArrayType>,
    peer_addr_stream: ColumnWriterImpl<'a, ByteArrayType>,
}

pub struct ParquetStreamClosed {
    session_id_close: ColumnCloseResult,
    peer_addr_close: ColumnCloseResult,
}

pub struct ParquetStreamBytes {
    properties: WriterPropertiesPtr,
    schema: TypePtr,
    session_id: Bytes,
    peer_addr: Bytes,
}

impl ParquetStreamWriter<'_> {
    fn close(self) -> parquet::errors::Result<ParquetStreamClosed> {
        Ok(ParquetStreamClosed {
            session_id_close: self.session_id_stream.close()?,
            peer_addr_close: self.peer_addr_stream.close()?,
        })
    }

    fn size(&self) -> u64 {
        self.session_id_stream.get_total_bytes_written()
            + self.peer_addr_stream.get_total_bytes_written()
    }
}

// why doesn't BytesMut impl io::Write?
struct BytesWriter {
    buf: BytesMut,
}

impl std::io::Write for BytesWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn take_write(w: &mut TrackedWrite<BytesWriter>) -> BytesWriter {
    std::mem::replace(
        w,
        TrackedWrite::new(BytesWriter {
            buf: BytesMut::new(),
        }),
    )
    .into_inner()
    .unwrap()
}

fn take_bytes(w: &mut TrackedWrite<BytesWriter>) -> Bytes {
    let mut stream = take_write(w);
    let bytes = stream.buf.split().freeze();
    *w = TrackedWrite::new(stream);
    bytes
}

impl ParquetStreamOwned {
    fn start(&mut self) -> ParquetStreamWriter<'_> {
        ParquetStreamWriter {
            session_id_stream: ColumnWriterImpl::new(
                self.schema.column(0),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.session_id_stream)),
            ),
            peer_addr_stream: ColumnWriterImpl::new(
                self.schema.column(1),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.peer_addr_stream)),
            ),
        }
    }

    fn flush(&mut self) -> parquet::errors::Result<ParquetStreamBytes> {
        Ok(ParquetStreamBytes {
            schema: self.schema.root_schema_ptr(),
            properties: self.properties.clone(),
            session_id: take_bytes(&mut self.session_id_stream),
            peer_addr: take_bytes(&mut self.peer_addr_stream),
        })
    }

    pub async fn worker(mut self, mut rx: Receiver<RequestContext>) -> parquet::errors::Result<()> {
        let tracker = TaskTracker::new();
        let mut columns = self.start();
        while let Some(ctx) = rx.recv().await {
            // columns.write....

            if columns.size() > 100_000_000 {
                let closed = columns.close()?;
                let bytes = self.flush()?;
                columns = self.start();

                tracker.spawn_blocking(|| bytes.write_to(closed));
            }
        }

        // closed
        let closed = columns.close()?;
        let bytes = self.flush()?;

        tracker.spawn_blocking(|| bytes.write_to(closed));
        tracker.close();
        tracker.wait().await;

        Ok(())
    }
}

impl ParquetStreamBytes {
    fn write_to(self, closed: ParquetStreamClosed) {
        self.write(closed, vec![]).unwrap();
    }

    fn write(
        self,
        closed: ParquetStreamClosed,
        w: impl std::io::Write + Send,
    ) -> parquet::errors::Result<FileMetaData> {
        let mut w = SerializedFileWriter::new(w, self.schema, self.properties)?;
        let mut g = w.next_row_group()?;
        g.append_column(&self.session_id, closed.session_id_close)?;
        g.append_column(&self.peer_addr, closed.peer_addr_close)?;

        dbg!(g.close()?);
        Ok(dbg!(w.close()?))
    }
}
