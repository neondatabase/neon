use std::sync::Arc;

use anyhow::Context;
use bytes::BytesMut;
use futures::{Future, Stream, StreamExt};
use parquet::{
    basic::ZstdLevel,
    file::{
        properties::{WriterProperties, WriterPropertiesPtr},
        writer::SerializedFileWriter,
    },
    record::RecordWriter,
};
use remote_storage::{GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, Span};
use utils::backoff;

use super::{RequestMonitoring, LOG_CHAN};

// Occasional network issues and such can cause remote operations to fail, and
// that's expected. If a upload fails, we log it at info-level, and retry.
// But after FAILED_UPLOAD_WARN_THRESHOLD retries, we start to log it at WARN
// level instead, as repeated failures can mean a more serious problem. If it
// fails more than FAILED_UPLOAD_RETRIES times, we give up
pub(crate) const FAILED_UPLOAD_WARN_THRESHOLD: u32 = 3;
pub(crate) const FAILED_UPLOAD_MAX_RETRIES: u32 = 10;

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

impl From<RequestMonitoring> for RequestData {
    fn from(value: RequestMonitoring) -> Self {
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

    worker_inner(storage, rx, ParquetConfig::default()).await
}

struct ParquetConfig {
    propeties: WriterPropertiesPtr,
    rows_per_group: usize,
    file_size: i64,

    #[cfg(any(test, feature = "testing"))]
    test_remote_failures: u64,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            propeties: Arc::new(
                WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
                    .build(),
            ),
            // 8192 takes about 40 seconds in us-east-2 to fill up
            rows_per_group: 8192,
            // 100 MiB
            file_size: 100 * 1024 * 1024,

            #[cfg(any(test, feature = "testing"))]
            test_remote_failures: 0,
        }
    }
}

async fn worker_inner(
    storage: GenericRemoteStorage,
    rx: impl Stream<Item = RequestData>,
    config: ParquetConfig,
) -> anyhow::Result<()> {
    #[cfg(any(test, feature = "testing"))]
    let storage = if config.test_remote_failures > 0 {
        GenericRemoteStorage::unreliable_wrapper(storage, config.test_remote_failures)
    } else {
        storage
    };

    let mut rx = std::pin::pin!(rx);

    let mut rows = Vec::with_capacity(config.rows_per_group);

    let schema = rows.as_slice().schema()?;
    let file = BytesWriter::default();
    let mut w = SerializedFileWriter::new(file, schema.clone(), config.propeties.clone())?;

    while let Some(row) = rx.next().await {
        rows.push(row);
        if rows.len() == config.rows_per_group {
            (rows, w) = flush_rows(rows, w).await?;

            let len = w
                .flushed_row_groups()
                .iter()
                .map(|rg| rg.compressed_size())
                .sum::<i64>();
            if len > config.file_size {
                let file = upload_parquet(w, len, &storage).await?;
                w = SerializedFileWriter::new(file, schema.clone(), config.propeties.clone())?;
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
    let span = Span::current();
    let (mut rows, w) = tokio::task::spawn_blocking(move || {
        let _enter = span.enter();

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

    // I don't know how compute intensive this is, although it probably isn't much... better be safe than sorry.
    // finish method only available on the fork: https://github.com/apache/arrow-rs/issues/5253
    let (mut file, metadata) = tokio::task::spawn_blocking(move || w.finish())
        .await
        .unwrap()?;

    let data = file.buf.split().freeze();

    let compression = len as f64 / len_uncompressed as f64;
    let size = data.len();
    let id = uuid::Uuid::now_v7();

    info!(
        %id,
        rows = metadata.num_rows,
        size, compression, "uploading request parquet file"
    );

    let path = RemotePath::from_string(&format!("requests_{id}.parquet"))?;
    backoff::retry(
        || async {
            let stream = futures::stream::once(futures::future::ready(Ok(data.clone())));
            storage.upload(stream, data.len(), &path, None).await
        },
        |_e| false,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_UPLOAD_MAX_RETRIES,
        "request_data_upload",
        // we don't want cancellation to interrupt here, so we make a dummy cancel token
        backoff::Cancel::new(CancellationToken::new(), || anyhow::anyhow!("Cancelled")),
    )
    .await
    .context("request_data_upload")?;

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

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, sync::Arc};

    use camino::Utf8Path;
    use itertools::Itertools;
    use parquet::{
        basic::ZstdLevel,
        file::{
            properties::WriterProperties, reader::FileReader,
            serialized_reader::SerializedFileReader,
        },
    };
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use remote_storage::{GenericRemoteStorage, RemoteStorageConfig, RemoteStorageKind};

    use super::{worker_inner, ParquetConfig, RequestData};

    fn generate_request_data(rng: &mut impl Rng) -> RequestData {
        RequestData {
            session_id: uuid::Builder::from_random_bytes(rng.gen()).into_uuid(),
            peer_addr: Ipv4Addr::from(rng.gen::<[u8; 4]>()).to_string(),
            timestamp: chrono::NaiveDateTime::from_timestamp_millis(
                rng.gen_range(1703862754..1803862754),
            )
            .unwrap(),
            application_name: Some("test".to_owned()),
            username: Some(hex::encode(rng.gen::<[u8; 4]>())),
            endpoint_id: Some(hex::encode(rng.gen::<[u8; 4]>())),
            project: Some(hex::encode(rng.gen::<[u8; 4]>())),
            branch: Some(hex::encode(rng.gen::<[u8; 4]>())),
            protocol: "random",
            region: "tests",
        }
    }

    async fn run_test(tmpdir: &Utf8Path, config: ParquetConfig) -> Vec<(u64, usize, i64)> {
        let remote_storage_config = RemoteStorageConfig {
            storage: RemoteStorageKind::LocalFs(tmpdir.to_path_buf()),
        };
        let storage = GenericRemoteStorage::from_config(&remote_storage_config).unwrap();

        // stable rng
        let mut rng = StdRng::from_seed([0x39; 32]);
        let rx = std::iter::repeat_with(|| generate_request_data(&mut rng))
            .take(100_000)
            .collect_vec();

        worker_inner(storage, futures::stream::iter(rx), config)
            .await
            .unwrap();

        let mut files = std::fs::read_dir(tmpdir.as_std_path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect_vec();
        files.sort();

        files
            .into_iter()
            .map(|path| std::fs::File::open(tmpdir.as_std_path().join(path)).unwrap())
            .map(|file| {
                (
                    file.metadata().unwrap(),
                    SerializedFileReader::new(file).unwrap().metadata().clone(),
                )
            })
            .map(|(file_meta, parquet_meta)| {
                (
                    file_meta.len(),
                    parquet_meta.num_row_groups(),
                    parquet_meta.file_metadata().num_rows(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn verify_parquet_no_compression() {
        let tmpdir = camino_tempfile::tempdir().unwrap();

        let config = ParquetConfig {
            propeties: Arc::new(WriterProperties::new()),
            rows_per_group: 2_000,
            file_size: 1_000_000,
            test_remote_failures: 0,
        };

        let file_stats = run_test(tmpdir.path(), config).await;

        assert_eq!(
            file_stats,
            [
                (1186400, 6, 12000),
                (1186279, 6, 12000),
                (1186301, 6, 12000),
                (1186520, 6, 12000),
                (1186486, 6, 12000),
                (1186560, 6, 12000),
                (1186537, 6, 12000),
                (1186366, 6, 12000),
                (395418, 2, 4000),
            ],
        );

        tmpdir.close().unwrap();
    }

    #[tokio::test]
    async fn verify_parquet_min_compression() {
        let tmpdir = camino_tempfile::tempdir().unwrap();

        let config = ParquetConfig {
            propeties: Arc::new(
                WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
                    .build(),
            ),
            rows_per_group: 2_000,
            file_size: 1_000_000,
            test_remote_failures: 0,
        };

        let file_stats = run_test(tmpdir.path(), config).await;

        // with compression, there are fewer files with more rows per file
        assert_eq!(
            file_stats,
            [
                (1024754, 9, 18000),
                (1019868, 9, 18000),
                (1023771, 9, 18000),
                (1018223, 9, 18000),
                (1021727, 9, 18000),
                (568302, 5, 10000),
            ],
        );

        tmpdir.close().unwrap();
    }

    #[tokio::test]
    async fn verify_parquet_strong_compression() {
        let tmpdir = camino_tempfile::tempdir().unwrap();

        let config = ParquetConfig {
            propeties: Arc::new(
                WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(
                        ZstdLevel::try_new(10).unwrap(),
                    ))
                    .build(),
            ),
            rows_per_group: 2_000,
            file_size: 1_000_000,
            test_remote_failures: 0,
        };

        let file_stats = run_test(tmpdir.path(), config).await;

        // with strong compression, there are even fewer files with more rows per file
        assert_eq!(
            file_stats,
            [
                (1114282, 10, 20000),
                (1114441, 10, 20000),
                (1114420, 10, 20000),
                (1114353, 10, 20000),
                (1114591, 10, 20000),
            ],
        );

        tmpdir.close().unwrap();
    }

    #[tokio::test]
    async fn verify_parquet_unreliable() {
        let tmpdir = camino_tempfile::tempdir().unwrap();

        let config = ParquetConfig {
            propeties: Arc::new(WriterProperties::new()),
            rows_per_group: 2_000,
            file_size: 1_000_000,
            test_remote_failures: 2,
        };

        let file_stats = run_test(tmpdir.path(), config).await;

        assert_eq!(
            file_stats,
            [
                (1186400, 6, 12000),
                (1186279, 6, 12000),
                (1186301, 6, 12000),
                (1186520, 6, 12000),
                (1186486, 6, 12000),
                (1186560, 6, 12000),
                (1186537, 6, 12000),
                (1186366, 6, 12000),
                (395418, 2, 4000),
            ],
        );

        tmpdir.close().unwrap();
    }
}
