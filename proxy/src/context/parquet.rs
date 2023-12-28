use std::sync::Arc;

use bytes::{Bytes, BytesMut};
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
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::proxy::run_until_cancelled;

use super::{RequestContext, LOG_CHAN};

// the parquet crate leaves a lot to be desired...
// what follows is an attempt to write parquet files with minimal allocs.
// complication: parquet is a columnar format, while we want to write in as rows.
// design:
// * we have a fixed number of columns, so we can represent the file with N column writers
// * when we have written enough data, we can flush the columns into a single buffer to be uploaded
//
// We might be able to skip the second buffer and stream it directly to S3. Will need faff because no async API.

pub struct ParquetRequestStream {
    properties: WriterPropertiesPtr,
    schema: SchemaDescPtr,
    session_id: TrackedWrite<BytesWriter>,
    peer_addr: TrackedWrite<BytesWriter>,
    timestamp: TrackedWrite<BytesWriter>,
    username: TrackedWrite<BytesWriter>,
    application: TrackedWrite<BytesWriter>,
    endpoint: TrackedWrite<BytesWriter>,
}

/// Column writers for each column we have
pub struct ParquetStreamWriter<'a> {
    session_id: ColumnWriterImpl<'a, FixedLenByteArrayType>,
    peer_addr: ColumnWriterImpl<'a, ByteArrayType>,
    timestamp: ColumnWriterImpl<'a, Int64Type>,
    username: ColumnWriterImpl<'a, ByteArrayType>,
    application: ColumnWriterImpl<'a, ByteArrayType>,
    endpoint: ColumnWriterImpl<'a, ByteArrayType>,
}

/// Collection of closed columns
pub struct ParquetStreamClosed {
    session_id: ColumnCloseResult,
    peer_addr: ColumnCloseResult,
    timestamp: ColumnCloseResult,
    username: ColumnCloseResult,
    application: ColumnCloseResult,
    endpoint: ColumnCloseResult,
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

fn write_required_string(
    stream: &mut ColumnWriterImpl<'_, ByteArrayType>,
    s: impl Into<String>,
) -> parquet::errors::Result<()> {
    stream.write_batch(&[ByteArray::from(s.into().into_bytes())], None, None)?;
    Ok(())
}

fn write_optional_string(
    stream: &mut ColumnWriterImpl<'_, ByteArrayType>,
    s: Option<impl Into<String>>,
) -> parquet::errors::Result<()> {
    if let Some(s) = s {
        stream.write_batch(&[ByteArray::from(s.into().into_bytes())], Some(&[1]), None)?;
    } else {
        stream.write_batch(&[], Some(&[0]), None)?;
    }
    Ok(())
}

impl ParquetStreamWriter<'_> {
    fn close(self) -> parquet::errors::Result<ParquetStreamClosed> {
        Ok(ParquetStreamClosed {
            session_id: self.session_id.close()?,
            peer_addr: self.peer_addr.close()?,
            timestamp: self.timestamp.close()?,
            username: self.username.close()?,
            application: self.application.close()?,
            endpoint: self.endpoint.close()?,
        })
    }

    fn size(&self) -> u64 {
        self.session_id.get_total_bytes_written()
            + self.peer_addr.get_total_bytes_written()
            + self.timestamp.get_total_bytes_written()
    }

    fn write(&mut self, ctx: RequestContext) -> parquet::errors::Result<()> {
        self.session_id.write_batch(
            &[FixedLenByteArray::from(ctx.session_id.as_bytes().to_vec())],
            None,
            None,
        )?;
        write_required_string(&mut self.peer_addr, ctx.peer_addr.to_string())?;
        self.timestamp.write_batch(
            &[ctx.first_packet.timestamp_nanos_opt().unwrap()],
            None,
            None,
        )?;
        write_optional_string(&mut self.username, ctx.user.as_deref())?;
        write_optional_string(&mut self.application, ctx.application.as_deref())?;
        write_optional_string(&mut self.endpoint, ctx.endpoint_id.as_deref())?;
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

impl ParquetRequestStream {
    fn start(&mut self) -> ParquetStreamWriter<'_> {
        ParquetStreamWriter {
            session_id: ColumnWriterImpl::new(
                self.schema.column(0),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.session_id)),
            ),
            peer_addr: ColumnWriterImpl::new(
                self.schema.column(1),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.peer_addr)),
            ),
            timestamp: ColumnWriterImpl::new(
                self.schema.column(2),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.timestamp)),
            ),
            username: ColumnWriterImpl::new(
                self.schema.column(3),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.username)),
            ),
            application: ColumnWriterImpl::new(
                self.schema.column(4),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.application)),
            ),
            endpoint: ColumnWriterImpl::new(
                self.schema.column(5),
                self.properties.clone(),
                Box::new(SerializedPageWriter::new(&mut self.endpoint)),
            ),
        }
    }

    fn flush(&mut self) -> parquet::errors::Result<ParquetStreamBytes> {
        Ok(ParquetStreamBytes {
            schema: self.schema.root_schema_ptr(),
            properties: self.properties.clone(),
            session_id: take_bytes(&mut self.session_id),
            peer_addr: take_bytes(&mut self.peer_addr),
            timestamp: take_bytes(&mut self.timestamp),
            username: take_bytes(&mut self.username),
            application: take_bytes(&mut self.application),
            endpoint: take_bytes(&mut self.endpoint),
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
            session_id: TrackedWrite::new(BytesWriter::default()),
            peer_addr: TrackedWrite::new(BytesWriter::default()),
            timestamp: TrackedWrite::new(BytesWriter::default()),
            username: TrackedWrite::new(BytesWriter::default()),
            application: TrackedWrite::new(BytesWriter::default()),
            endpoint: TrackedWrite::new(BytesWriter::default()),
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
        g.append_column(&self.session_id, closed.session_id)?;
        g.append_column(&self.peer_addr, closed.peer_addr)?;
        g.append_column(&self.timestamp, closed.timestamp)?;
        g.append_column(&self.username, closed.username)?;
        g.append_column(&self.application, closed.application)?;
        g.append_column(&self.endpoint, closed.endpoint)?;

        g.close()?;
        w.close()
    }
}
