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

impl From<RequestContext> for RequestData {
    fn from(value: RequestContext) -> Self {
        Self {
            session_id: value.session_id,
            peer_addr: value.peer_addr.to_string(),
            timestamp: value.first_packet,
            username: value.user.as_deref().map(String::from),
            application_name: value.application.as_deref().map(String::from),
            endpoint_id: value.endpoint_id.as_deref().map(String::from),
        }
    }
}

/// Parquet request context worker
///
/// It listened on a channel for all completed requests, extracts the data and writes it into a parquet file,
/// then uploads a completed batch to S3
pub async fn worker(cancellation_token: CancellationToken) -> parquet::errors::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    LOG_CHAN.set(tx).unwrap();

    let schema = RequestData::schema();
    let root = schema.root_schema_ptr();
    let properties = Arc::new(WriterProperties::builder().build());

    let mut b = ColumnBytesWriters::new()?;

    let tracker = TaskTracker::new();
    let mut columns = b.start(properties.clone(), schema.clone());

    while let Some(Some(ctx)) = run_until_cancelled(rx.recv(), &cancellation_token).await {
        columns.write(ctx.into())?;

        if columns.size() > 1_000_000 {
            let closed = columns.close()?;
            let bytes = b.flush()?;
            columns = b.start(properties.clone(), schema.clone());

            let root = root.clone();
            let props = properties.clone();
            tracker.spawn_blocking(|| bytes.write_to(closed, root, props));
        }
    }

    // drain
    rx.close();

    while let Some(ctx) = rx.recv().await {
        columns.write(ctx.into())?;
    }

    // closed
    let closed = columns.close()?;
    let bytes = b.flush()?;

    tracker.spawn_blocking(|| bytes.write_to(closed, root, properties));
    tracker.close();
    tracker.wait().await;

    Ok(())
}

impl ColumnBytes {
    fn write_to(
        self,
        closed: ClosedColumns,
        schema_root: TypePtr,
        properties: WriterPropertiesPtr,
    ) {
        // std::fs::create_dir_all("parquet_test").unwrap();
        // let file = std::fs::File::create(format!("parquet_test/{}.parquet", Utc::now())).unwrap();
        // self.write(closed, file).unwrap();
        self.write(
            closed,
            SerializedFileWriter::new(vec![], schema_root, properties).unwrap(),
        )
        .unwrap();
    }
}

/// this macro reduces the repitition required with all the different columns we store
macro_rules! build_column_writers {
    (
        struct RequestData {$(
            $name:ident: $ty:ty,
        )*}
    ) => {
        struct RequestData {$(
            $name: $ty,
        )*}

        impl RequestData {
            fn schema() -> SchemaDescPtr {
                Arc::new(SchemaDescriptor::new(Arc::new(
                    Type::group_type_builder("schema")
                        .with_fields(vec![$(
                            Arc::new(<$ty>::get_type(stringify!($name))),
                        )*])
                        .build()
                        .unwrap()
                )))
            }
        }

        /// The `io::Write`rs for each column
        struct ColumnBytesWriters {$(
            $name: TrackedWrite<BytesWriter>,
        )*}

        /// The bytes for each completed column
        struct ColumnBytes {$(
            $name: Bytes,
        )*}

        /// The parquet writer abstraction for each column
        struct ColumnWriters<'a> {$(
            $name: ColumnWriterImpl<'a, <$ty as ParquetType>::PhysicalType>,
        )*}

        impl ColumnWriters<'_> {
            fn close(self) -> parquet::errors::Result<ClosedColumns> {
                Ok(ClosedColumns {$(
                    $name: self.$name.close()?,
                )*})
            }

            /// Note: this value does not include any buffered data that has not yet been flushed to a page.
            fn size(&self) -> u64 {
                0 $( + self.$name.get_total_bytes_written() )*
            }

            fn write(&mut self, ctx: RequestData) -> parquet::errors::Result<()> {
                $(ctx.$name.write(&mut self.$name, false)?;)*
                Ok(())
            }
        }

        /// The metadata of each closed parquet column
        struct ClosedColumns {$(
            $name: ColumnCloseResult,
        )*}

        impl ColumnBytes {
            fn write(
                self,
                closed: ClosedColumns,
                mut w: SerializedFileWriter<impl std::io::Write + Send>,
            ) -> parquet::errors::Result<FileMetaData> {
                let mut g = w.next_row_group()?;

                // write each column in order to the row group
                $(g.append_column(&self.$name, closed.$name)?;)*

                g.close()?;
                w.close()
            }
        }

        impl ColumnBytesWriters {
            fn start(
                &mut self,
                properties: WriterPropertiesPtr,
                schema: SchemaDescPtr,
            ) -> ColumnWriters<'_> {
                let mut n = 0;
                ColumnWriters {$(
                    $name: ColumnWriterImpl::new(
                        schema.column({ n += 1; n - 1 }),
                        properties.clone(),
                        Box::new(SerializedPageWriter::new(&mut self.$name)),
                    ),
                )*}
            }

            fn flush(&mut self) -> parquet::errors::Result<ColumnBytes> {
                Ok(ColumnBytes {$(
                    $name: take_bytes(&mut self.$name),
                )*})
            }

            fn new() -> parquet::errors::Result<Self> {
                Ok(Self {$(
                    $name: TrackedWrite::new(BytesWriter::default()),
                )*})
            }
        }
    };
}

build_column_writers!(
    struct RequestData {
        session_id: uuid::Uuid,
        peer_addr: String,
        timestamp: chrono::DateTime<chrono::Utc>,
        username: Option<String>,
        application_name: Option<String>,
        endpoint_id: Option<String>,
    }
);

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

trait ParquetType {
    type PhysicalType: parquet::data_type::DataType;
    fn get_type(name: &str) -> Type;
    fn write(
        self,
        w: &mut ColumnWriterImpl<'_, Self::PhysicalType>,
        optional: bool,
    ) -> parquet::errors::Result<usize>;
}

impl ParquetType for String {
    type PhysicalType = ByteArrayType;

    fn get_type(name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap()
    }

    fn write(
        self,
        w: &mut ColumnWriterImpl<'_, Self::PhysicalType>,
        optional: bool,
    ) -> parquet::errors::Result<usize> {
        let val = &[ByteArray::from(self.as_bytes().to_vec())];
        let def = optional.then_some(&1_i16).map(std::slice::from_ref);
        w.write_batch(val, def, None)
    }
}

impl ParquetType for uuid::Uuid {
    type PhysicalType = FixedLenByteArrayType;

    fn get_type(name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(16)
            .with_logical_type(Some(LogicalType::String))
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap()
    }

    fn write(
        self,
        w: &mut ColumnWriterImpl<'_, Self::PhysicalType>,
        optional: bool,
    ) -> parquet::errors::Result<usize> {
        let val = &[FixedLenByteArray::from(self.as_bytes().to_vec())];
        let def = optional.then_some(&1_i16).map(std::slice::from_ref);
        w.write_batch(val, def, None)
    }
}

impl ParquetType for chrono::DateTime<chrono::Utc> {
    type PhysicalType = Int64Type;

    fn get_type(name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::format::TimeUnit::NANOS(NanoSeconds::new()),
            }))
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap()
    }

    fn write(
        self,
        w: &mut ColumnWriterImpl<'_, Self::PhysicalType>,
        optional: bool,
    ) -> parquet::errors::Result<usize> {
        let val = &[self.timestamp_nanos_opt().unwrap()];
        let def = optional.then_some(&1_i16).map(std::slice::from_ref);
        w.write_batch(val, def, None)
    }
}

impl<T: ParquetType> ParquetType for Option<T> {
    type PhysicalType = T::PhysicalType;

    fn get_type(name: &str) -> Type {
        let t = T::get_type(name);
        Type::primitive_type_builder(t.name(), t.get_physical_type())
            .with_logical_type(t.get_basic_info().logical_type())
            .with_converted_type(t.get_basic_info().converted_type())
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap()
    }

    fn write(
        self,
        w: &mut ColumnWriterImpl<'_, Self::PhysicalType>,
        _optional: bool,
    ) -> parquet::errors::Result<usize> {
        if let Some(s) = self {
            s.write(w, true)
        } else {
            w.write_batch(&[], Some(&[0]), None)
        }
    }
}
