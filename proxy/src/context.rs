//! Connection request contexts

use std::{net::IpAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use once_cell::sync::OnceCell;
use parquet::{
    basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType},
    column::writer::{ColumnCloseResult, ColumnWriterImpl},
    data_type::{ByteArray, ByteArrayType, FixedLenByteArray, FixedLenByteArrayType, Int64Type},
    file::{
        properties::{WriterProperties, WriterPropertiesPtr},
        writer::{SerializedFileWriter, SerializedPageWriter, TrackedWrite},
    },
    format::{FileMetaData, NanoSeconds},
    schema::types::{SchemaDescPtr, SchemaDescriptor, Type, TypePtr},
};
use serde::{Serialize, Serializer};
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use crate::{error::ErrorKind, metrics::LatencyTimer, proxy::run_until_cancelled};

static LOG_CHAN: OnceCell<mpsc::UnboundedSender<RequestContext>> = OnceCell::new();

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
            if let Some(tx) = LOG_CHAN.get() {
                let _: Result<(), _> = tx.send(self.clone());
            }
            // info!("{}", serde_json::to_string(self).unwrap());
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
    timestamp_stream: TrackedWrite<BytesWriter>,
    username_stream: TrackedWrite<BytesWriter>,
    application_stream: TrackedWrite<BytesWriter>,
    endpoint_stream: TrackedWrite<BytesWriter>,
}

/// Column writers for each column we have
pub struct ParquetStreamWriter<'a> {
    session_id_stream: ColumnWriterImpl<'a, FixedLenByteArrayType>,
    peer_addr_stream: ColumnWriterImpl<'a, ByteArrayType>,
    timestamp_stream: ColumnWriterImpl<'a, Int64Type>,
    username_stream: ColumnWriterImpl<'a, ByteArrayType>,
    application_stream: ColumnWriterImpl<'a, ByteArrayType>,
    endpoint_stream: ColumnWriterImpl<'a, ByteArrayType>,
}

pub struct ParquetStreamClosed {
    session_id_close: ColumnCloseResult,
    peer_addr_close: ColumnCloseResult,
    timestamp_close: ColumnCloseResult,
    username_close: ColumnCloseResult,
    application_close: ColumnCloseResult,
    endpoint_close: ColumnCloseResult,
}

pub struct ParquetStreamBytes {
    properties: WriterPropertiesPtr,
    schema: TypePtr,
    session_id: Bytes,
    peer_addr: Bytes,
    timestamp: Bytes,
    username: Bytes,
    application: Bytes,
    endpoint: Bytes,
}

impl ParquetStreamWriter<'_> {
    fn close(self) -> parquet::errors::Result<ParquetStreamClosed> {
        Ok(ParquetStreamClosed {
            session_id_close: self.session_id_stream.close()?,
            peer_addr_close: self.peer_addr_stream.close()?,
            timestamp_close: self.timestamp_stream.close()?,
            username_close: self.username_stream.close()?,
            application_close: self.application_stream.close()?,
            endpoint_close: self.endpoint_stream.close()?,
        })
    }

    fn size(&self) -> u64 {
        self.session_id_stream.get_total_bytes_written()
            + self.peer_addr_stream.get_total_bytes_written()
            + self.timestamp_stream.get_total_bytes_written()
    }

    fn write(&mut self, ctx: RequestContext) -> parquet::errors::Result<()> {
        self.session_id_stream.write_batch(
            &[FixedLenByteArray::from(ctx.session_id.as_bytes().to_vec())],
            None,
            None,
        )?;
        self.peer_addr_stream.write_batch(
            &[ByteArray::from(ctx.peer_addr.to_string().into_bytes())],
            None,
            None,
        )?;
        self.timestamp_stream.write_batch(
            &[ctx.first_packet.timestamp_nanos_opt().unwrap()],
            None,
            None,
        )?;
        if let Some(username) = &ctx.user {
            self.username_stream.write_batch(
                &[ByteArray::from(username.as_bytes().to_vec())],
                Some(&[1]),
                None,
            )?;
        } else {
            self.username_stream.write_batch(&[], Some(&[0]), None)?;
        }
        if let Some(application) = &ctx.application {
            self.application_stream.write_batch(
                &[ByteArray::from(application.as_bytes().to_vec())],
                Some(&[1]),
                None,
            )?;
        } else {
            self.application_stream.write_batch(&[], Some(&[0]), None)?;
        }
        if let Some(endpoint) = &ctx.endpoint_id {
            self.endpoint_stream.write_batch(
                &[ByteArray::from(endpoint.as_bytes().to_vec())],
                Some(&[1]),
                None,
            )?;
        } else {
            self.endpoint_stream.write_batch(&[], Some(&[0]), None)?;
        }
        Ok(())
    }
}

// why doesn't BytesMut impl io::Write?
#[derive(Default)]
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
    std::mem::replace(w, TrackedWrite::new(BytesWriter::default()))
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
            timestamp_stream: ColumnWriterImpl::new(
                self.schema.column(2),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.timestamp_stream)),
            ),
            username_stream: ColumnWriterImpl::new(
                self.schema.column(3),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.username_stream)),
            ),
            application_stream: ColumnWriterImpl::new(
                self.schema.column(4),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.application_stream)),
            ),
            endpoint_stream: ColumnWriterImpl::new(
                self.schema.column(5),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.endpoint_stream)),
            ),
        }
    }

    fn flush(&mut self) -> parquet::errors::Result<ParquetStreamBytes> {
        Ok(ParquetStreamBytes {
            schema: self.schema.root_schema_ptr(),
            properties: self.properties.clone(),
            session_id: take_bytes(&mut self.session_id_stream),
            peer_addr: take_bytes(&mut self.peer_addr_stream),
            timestamp: take_bytes(&mut self.timestamp_stream),
            username: take_bytes(&mut self.username_stream),
            application: take_bytes(&mut self.application_stream),
            endpoint: take_bytes(&mut self.endpoint_stream),
        })
    }

    pub fn new() -> parquet::errors::Result<Self> {
        let session_id = Arc::new(
            Type::primitive_type_builder("session_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(16)
                .with_logical_type(Some(LogicalType::Uuid))
                .with_repetition(Repetition::REQUIRED)
                .build()?,
        );

        let peer_addr = Arc::new(
            Type::primitive_type_builder("peer_addr", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .with_repetition(Repetition::REQUIRED)
                .build()?,
        );

        let timestamp = Arc::new(
            Type::primitive_type_builder("timestamp", PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: parquet::format::TimeUnit::NANOS(NanoSeconds::new()),
                }))
                .with_repetition(Repetition::REQUIRED)
                .build()?,
        );

        let username = Arc::new(
            Type::primitive_type_builder("username", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
        );

        let application = Arc::new(
            Type::primitive_type_builder("application_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
        );

        let endpoint = Arc::new(
            Type::primitive_type_builder("endpoint_id", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
        );

        let schema = Arc::new(
            Type::group_type_builder("proxy_connection_requests")
                .with_fields(vec![
                    session_id,
                    peer_addr,
                    timestamp,
                    username,
                    application,
                    endpoint,
                ])
                .build()?,
        );

        Ok(Self {
            properties: Arc::new(WriterProperties::builder().build()),
            schema: Arc::new(SchemaDescriptor::new(schema)),
            session_id_stream: TrackedWrite::new(BytesWriter::default()),
            peer_addr_stream: TrackedWrite::new(BytesWriter::default()),
            timestamp_stream: TrackedWrite::new(BytesWriter::default()),
            username_stream: TrackedWrite::new(BytesWriter::default()),
            application_stream: TrackedWrite::new(BytesWriter::default()),
            endpoint_stream: TrackedWrite::new(BytesWriter::default()),
        })
    }

    pub async fn worker(
        mut self,
        cancellation_token: CancellationToken,
    ) -> parquet::errors::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        LOG_CHAN.set(tx).unwrap();

        let tracker = TaskTracker::new();
        let mut columns = self.start();

        while let Some(Some(ctx)) = run_until_cancelled(rx.recv(), &cancellation_token).await {
            columns.write(ctx)?;

            if columns.size() > 1_000_000 {
                let closed = columns.close()?;
                let bytes = self.flush()?;
                columns = self.start();

                tracker.spawn_blocking(|| bytes.write_to(closed));
            }
        }

        // drain
        rx.close();

        while let Some(ctx) = rx.recv().await {
            columns.write(ctx)?;
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
        // std::fs::create_dir_all("parquet_test").unwrap();
        // let file = std::fs::File::create(format!("parquet_test/{}.parquet", Utc::now())).unwrap();
        // self.write(closed, file).unwrap();
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
        g.append_column(&self.timestamp, closed.timestamp_close)?;
        g.append_column(&self.username, closed.username_close)?;
        g.append_column(&self.application, closed.application_close)?;
        g.append_column(&self.endpoint, closed.endpoint_close)?;

        g.close()?;
        w.close()
    }
}
