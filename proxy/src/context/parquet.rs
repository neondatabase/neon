use std::sync::Arc;

use anyhow::Context;
use bytes::BytesMut;
use futures::{Future, Stream, StreamExt};
use parquet::{
    basic::ZstdLevel,
    file::{
        properties::WriterProperties, reader::FileReader, serialized_reader::SerializedFileReader,
        writer::SerializedFileWriter,
    },
    record::RecordWriter,
};
use remote_storage::{GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::{RequestContext, LOG_CHAN};

// the parquet crate leaves a lot to be desired...
// what follows is an attempt to write parquet files with minimal allocs.
// complication: parquet is a columnar format, while we want to write in as rows.
// design:
// * we batch up to 1024 rows, then flush them into a 'row group'
// * after each rowgroup write, we check the length of the file and upload to s3 if large enough

#[derive(parquet_derive::ParquetRecordWriter)]
struct RequestData {
    session_id: uuid::Uuid,
    peer_addr: String,
    /// Must be UTC. The derive macro doesn't like the timezones
    timestamp: chrono::NaiveDateTime,
    username: Option<String>,
    application_name: Option<String>,
    endpoint_id: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    protocol: &'static str,
    region: &'static str,
}

impl From<RequestContext> for RequestData {
    fn from(value: RequestContext) -> Self {
        Self {
            session_id: value.session_id,
            peer_addr: value.peer_addr.to_string(),
            timestamp: value.first_packet.naive_utc(),
            username: value.user.as_deref().map(String::from),
            application_name: value.application.as_deref().map(String::from),
            endpoint_id: value.endpoint_id.as_deref().map(String::from),
            project: value.project.as_deref().map(String::from),
            branch: value.branch.as_deref().map(String::from),
            protocol: value.protocol,
            region: value.region,
        }
    }
}

/// Parquet request context worker
///
/// It listened on a channel for all completed requests, extracts the data and writes it into a parquet file,
/// then uploads a completed batch to S3
pub async fn worker(cancellation_token: CancellationToken) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    LOG_CHAN.set(tx).unwrap();

    // setup S3 storage
    let remote_storage_config = RemoteStorageConfig {
        // storage: RemoteStorageKind::AwsS3(remote_storage::S3Config {
        //     bucket_name: "foo".to_owned(),
        //     bucket_region: "us-east-2".to_owned(),
        //     prefix_in_bucket: Some("proxy_requests/".to_owned()),
        //     endpoint: None,
        //     concurrency_limit: std::num::NonZeroUsize::new(100).unwrap(),
        //     max_keys_per_list_response: None,
        // }),
        storage: RemoteStorageKind::LocalFs(camino::Utf8PathBuf::from("parquet_test/")),
    };
    let storage =
        GenericRemoteStorage::from_config(&remote_storage_config).context("remote storage init")?;

    // setup row stream that will close on cancellation
    let mut cancelled = std::pin::pin!(cancellation_token.cancelled());
    let rx = futures::stream::poll_fn(move |cx| {
        // `cancelled`` is 'fused'
        if cancelled.as_mut().poll(cx).is_ready() {
            rx.close();
        }
        rx.poll_recv(cx)
    });
    let rx = rx.map(RequestData::from);

    worker_inner(storage, rx).await
}

async fn worker_inner(
    storage: GenericRemoteStorage,
    rx: impl Stream<Item = RequestData>,
) -> anyhow::Result<()> {
    let mut rx = std::pin::pin!(rx);
    let mut rows = Vec::with_capacity(1024);

    let schema = rows.as_slice().schema()?;
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
            .set_data_page_size_limit(1024 * 1024)
            .set_write_batch_size(1024)
            .build(),
    );

    let file = BytesWriter::default();
    let mut w = SerializedFileWriter::new(file, schema.clone(), properties.clone()).unwrap();

    while let Some(row) = rx.next().await {
        rows.push(row);
        if rows.len() == 1024 {
            (rows, w) = flush_rows(rows, w).await?;

            let len = w
                .flushed_row_groups()
                .iter()
                .map(|rg| rg.compressed_size())
                .sum::<i64>();
            if len > 1024 * 1024 {
                let file = upload_parquet(w, len, &storage).await?;
                w = SerializedFileWriter::new(file, schema.clone(), properties.clone())?;
            }
        }
    }

    if !rows.is_empty() {
        (_, w) = flush_rows(rows, w).await?;
    }

    if !w.flushed_row_groups().is_empty() {
        let len = w
            .flushed_row_groups()
            .iter()
            .map(|rg| rg.compressed_size())
            .sum::<i64>();
        let _: BytesWriter = upload_parquet(w, len, &storage).await?;
    }

    Ok(())
}

async fn flush_rows(
    rows: Vec<RequestData>,
    mut w: SerializedFileWriter<BytesWriter>,
) -> anyhow::Result<(Vec<RequestData>, SerializedFileWriter<BytesWriter>)> {
    let (mut rows, w) = tokio::task::spawn_blocking(move || {
        let mut rg = w.next_row_group()?;
        rows.as_slice().write_to_row_group(&mut rg)?;
        let meta = rg.close()?;

        let size = meta.compressed_size();
        let compression = meta.compressed_size() as f64 / meta.total_byte_size() as f64;

        debug!(size, compression, "flushed row group to parquet file");

        Ok::<_, parquet::errors::ParquetError>((rows, w))
    })
    .await
    .unwrap()?;

    rows.clear();
    Ok((rows, w))
}

async fn upload_parquet(
    w: SerializedFileWriter<BytesWriter>,
    len: i64,
    storage: &GenericRemoteStorage,
) -> anyhow::Result<BytesWriter> {
    let len_uncompressed = w
        .flushed_row_groups()
        .iter()
        .map(|rg| rg.total_byte_size())
        .sum::<i64>();

    // I don't know how compute intensive this is, although it probably isn't much...
    let mut file = tokio::task::spawn_blocking(move || w.into_inner())
        .await
        .unwrap()?;

    let data = file.buf.split().freeze();

    // get metadata: https://github.com/apache/arrow-rs/issues/5253
    let metadata = SerializedFileReader::new(data.clone())?
        .metadata()
        .file_metadata()
        .clone();

    let compression = len as f64 / len_uncompressed as f64;
    let size = data.len();
    let id = uuid::Uuid::now_v7();

    info!(
        %id,
        rows = metadata.num_rows(),
        size, compression, "uploading request parquet file"
    );

    let path = RemotePath::from_string(&format!("requests_{id}.parquet"))?;
    storage
        .upload(futures::stream::iter(Some(Ok(data))), size, &path, None)
        .await?;

    Ok(file)
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
