use std::sync::Arc;

use anyhow::Context;
use bytes::BytesMut;
use futures::{Future, Stream, StreamExt};
use parquet::{
    basic::Compression,
    file::{
        metadata::RowGroupMetaDataPtr,
        properties::{WriterProperties, WriterPropertiesPtr, DEFAULT_PAGE_SIZE},
        writer::SerializedFileWriter,
    },
    record::RecordWriter,
};
use remote_storage::{GenericRemoteStorage, RemotePath, RemoteStorageConfig};
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, Span};
use utils::backoff;

use super::{RequestMonitoring, LOG_CHAN};

#[derive(clap::Args, Clone, Debug)]
pub struct ParquetUploadArgs {
    /// Storage location to upload the parquet files to.
    /// Encoded as toml (same format as pageservers), eg
    /// `{bucket_name='the-bucket',bucket_region='us-east-1',prefix_in_bucket='proxy',endpoint='http://minio:9000'}`
    #[clap(long, value_parser = remote_storage_from_toml)]
    parquet_upload_remote_storage: Option<RemoteStorageConfig>,

    /// How many rows to include in a row group
    #[clap(long, default_value_t = 8192)]
    parquet_upload_row_group_size: usize,

    /// How large each column page should be in bytes
    #[clap(long, default_value_t = DEFAULT_PAGE_SIZE)]
    parquet_upload_page_size: usize,

    /// How large the total parquet file should be in bytes
    #[clap(long, default_value_t = 100_000_000)]
    parquet_upload_size: i64,

    /// How long to wait before forcing a file upload
    #[clap(long, default_value = "20m", value_parser = humantime::parse_duration)]
    parquet_upload_maximum_duration: tokio::time::Duration,

    /// What level of compression to use
    #[clap(long, default_value_t = Compression::UNCOMPRESSED)]
    parquet_upload_compression: Compression,
}

fn remote_storage_from_toml(s: &str) -> anyhow::Result<Option<RemoteStorageConfig>> {
    RemoteStorageConfig::from_toml(&s.parse()?)
}

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
pub async fn worker(
    cancellation_token: CancellationToken,
    config: ParquetUploadArgs,
) -> anyhow::Result<()> {
    let Some(remote_storage_config) = config.parquet_upload_remote_storage else {
        tracing::warn!("parquet request upload: no s3 bucket configured");
        return Ok(());
    };

    let (tx, mut rx) = mpsc::unbounded_channel();
    LOG_CHAN.set(tx).unwrap();

    let storage =
        GenericRemoteStorage::from_config(&remote_storage_config).context("remote storage init")?;

    // setup row stream that will close on cancellation
    let mut cancelled = std::pin::pin!(cancellation_token.cancelled());
    let rx = futures::stream::poll_fn(move |cx| {
        // `cancelled` is 'fused'
        if cancelled.as_mut().poll(cx).is_ready() {
            rx.close();
        }
        rx.poll_recv(cx)
    });
    let rx = rx.map(RequestData::from);

    let properties = WriterProperties::builder()
        .set_data_page_size_limit(config.parquet_upload_page_size)
        .set_compression(config.parquet_upload_compression);

    let parquet_config = ParquetConfig {
        propeties: Arc::new(properties.build()),
        rows_per_group: config.parquet_upload_row_group_size,
        file_size: config.parquet_upload_size,
        max_duration: config.parquet_upload_maximum_duration,

        #[cfg(any(test, feature = "testing"))]
        test_remote_failures: 0,
    };

    worker_inner(storage, rx, parquet_config).await
}

struct ParquetConfig {
    propeties: WriterPropertiesPtr,
    rows_per_group: usize,
    file_size: i64,

    max_duration: tokio::time::Duration,

    #[cfg(any(test, feature = "testing"))]
    test_remote_failures: u64,
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

    let mut last_upload = time::Instant::now();

    let mut len = 0;
    while let Some(row) = rx.next().await {
        rows.push(row);
        let force = last_upload.elapsed() > config.max_duration;
        if rows.len() == config.rows_per_group || force {
            let rg_meta;
            (rows, w, rg_meta) = flush_rows(rows, w).await?;
            len += rg_meta.compressed_size();
        }
        if len > config.file_size || force {
            last_upload = time::Instant::now();
            let file = upload_parquet(w, len, &storage).await?;
            w = SerializedFileWriter::new(file, schema.clone(), config.propeties.clone())?;
            len = 0;
        }
    }

    if !rows.is_empty() {
        let rg_meta;
        (_, w, rg_meta) = flush_rows(rows, w).await?;
        len += rg_meta.compressed_size();
    }

    if !w.flushed_row_groups().is_empty() {
        let _: BytesWriter = upload_parquet(w, len, &storage).await?;
    }

    Ok(())
}

async fn flush_rows(
    rows: Vec<RequestData>,
    mut w: SerializedFileWriter<BytesWriter>,
) -> anyhow::Result<(
    Vec<RequestData>,
    SerializedFileWriter<BytesWriter>,
    RowGroupMetaDataPtr,
)> {
    let span = Span::current();
    let (mut rows, w, rg_meta) = tokio::task::spawn_blocking(move || {
        let _enter = span.enter();

        let mut rg = w.next_row_group()?;
        rows.as_slice().write_to_row_group(&mut rg)?;
        let rg_meta = rg.close()?;

        let size = rg_meta.compressed_size();
        let compression = rg_meta.compressed_size() as f64 / rg_meta.total_byte_size() as f64;

        debug!(size, compression, "flushed row group to parquet file");

        Ok::<_, parquet::errors::ParquetError>((rows, w, rg_meta))
    })
    .await
    .unwrap()?;

    rows.clear();
    Ok((rows, w, rg_meta))
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
    use futures::{Stream, StreamExt};
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
    use tokio::{sync::mpsc, task::yield_now, time};

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
            endpoint_id: Some(hex::encode(rng.gen::<[u8; 16]>())),
            project: Some(hex::encode(rng.gen::<[u8; 16]>())),
            branch: Some(hex::encode(rng.gen::<[u8; 16]>())),
            protocol: ["tcp", "ws", "http"][rng.gen_range(0..3)],
            region: "us-east-1",
        }
    }

    fn random_stream(len: usize) -> impl Stream<Item = RequestData> + Unpin {
        let mut rng = StdRng::from_seed([0x39; 32]);
        futures::stream::iter(
            std::iter::repeat_with(move || generate_request_data(&mut rng)).take(len),
        )
    }

    async fn run_test(
        tmpdir: &Utf8Path,
        config: ParquetConfig,
        rx: impl Stream<Item = RequestData>,
    ) -> Vec<(u64, usize, i64)> {
        let remote_storage_config = RemoteStorageConfig {
            storage: RemoteStorageKind::LocalFs(tmpdir.to_path_buf()),
        };
        let storage = GenericRemoteStorage::from_config(&remote_storage_config).unwrap();

        worker_inner(storage, rx, config).await.unwrap();

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
            max_duration: time::Duration::from_secs(20 * 60),
            test_remote_failures: 0,
        };

        let rx = random_stream(50_000);
        let file_stats = run_test(tmpdir.path(), config, rx).await;

        assert_eq!(
            file_stats,
            [
                (1028656, 3, 6000),
                (1028578, 3, 6000),
                (1028719, 3, 6000),
                (1028632, 3, 6000),
                (1028753, 3, 6000),
                (1028520, 3, 6000),
                (1028678, 3, 6000),
                (1028750, 3, 6000),
                (342955, 1, 2000)
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
            max_duration: time::Duration::from_secs(20 * 60),
            test_remote_failures: 0,
        };

        let rx = random_stream(50_000);
        let file_stats = run_test(tmpdir.path(), config, rx).await;

        // with compression, there are fewer files with more rows per file
        assert_eq!(
            file_stats,
            [
                (1165098, 6, 12000),
                (1162474, 6, 12000),
                (1163538, 6, 12000),
                (1167669, 6, 12000),
                (196574, 1, 2000)
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
            max_duration: time::Duration::from_secs(20 * 60),
            test_remote_failures: 0,
        };

        let rx = random_stream(50_000);
        let file_stats = run_test(tmpdir.path(), config, rx).await;

        // with strong compression, the files are smaller
        assert_eq!(
            file_stats,
            [
                (1143831, 6, 12000),
                (1143838, 6, 12000),
                (1143632, 6, 12000),
                (1143833, 6, 12000),
                (190848, 1, 2000)
            ],
        );

        tmpdir.close().unwrap();
    }

    #[tokio::test]
    async fn verify_parquet_unreliable_upload() {
        let tmpdir = camino_tempfile::tempdir().unwrap();

        let config = ParquetConfig {
            propeties: Arc::new(WriterProperties::new()),
            rows_per_group: 2_000,
            file_size: 1_000_000,
            max_duration: time::Duration::from_secs(20 * 60),
            test_remote_failures: 2,
        };

        let rx = random_stream(50_000);
        let file_stats = run_test(tmpdir.path(), config, rx).await;

        assert_eq!(
            file_stats,
            [
                (1028656, 3, 6000),
                (1028578, 3, 6000),
                (1028719, 3, 6000),
                (1028632, 3, 6000),
                (1028753, 3, 6000),
                (1028520, 3, 6000),
                (1028678, 3, 6000),
                (1028750, 3, 6000),
                (342955, 1, 2000)
            ],
        );

        tmpdir.close().unwrap();
    }

    #[tokio::test]
    async fn verify_parquet_regular_upload() {
        let tmpdir = camino_tempfile::tempdir().unwrap();

        let config = ParquetConfig {
            propeties: Arc::new(WriterProperties::new()),
            rows_per_group: 2_000,
            file_size: 1_000_000,
            max_duration: time::Duration::from_secs(60),
            test_remote_failures: 2,
        };

        time::pause();

        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            for _ in 0..3 {
                let mut s = random_stream(3000);
                while let Some(r) = s.next().await {
                    tx.send(r).unwrap();
                    yield_now().await;
                }
                let sleep = time::sleep(time::Duration::from_secs(61));
                time::advance(time::Duration::from_secs(61)).await;
                sleep.await
            }
        });

        let rx = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
        let file_stats = run_test(tmpdir.path(), config, rx).await;

        // files are smaller than the size threshold, but they took too long to fill so were flushed early
        assert_eq!(
            file_stats,
            [(515474, 2, 3001), (515252, 2, 3000), (515092, 2, 2999)],
        );

        tmpdir.close().unwrap();
    }
}
