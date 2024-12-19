//！ Google Cloud Storage wrapper

use crate::config::GcsConfig;
use crate::{
    Download, DownloadError, DownloadOpts, Listing, ListingMode, ListingObject, RemotePath,
    RemoteStorage, StorageMetadata, TimeTravelError,
};
use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use futures_util::{SinkExt, StreamExt};
use opendal::layers::{LoggingLayer, RetryLayer, TimeoutLayer};
use opendal::Operator;
use std::env;
use std::num::NonZeroU32;
use std::pin::pin;
use std::time::{Duration, SystemTime};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// TODO: features we can implement in the future
///
/// They are all available at opendal side but need to be integrated into this wrapper.
///
/// - connection pool size
/// - concurrent limit
/// - max keys per list response
pub struct GoogleCloudStorage {
    client: Operator,
}

impl GoogleCloudStorage {
    pub fn new(gcs_config: &GcsConfig, timeout: Duration) -> Result<Self> {
        debug!(
            "Creating gcs remote storage for gcs bucket {}",
            gcs_config.bucket_name
        );

        let mut cfg = opendal::services::GcsConfig::default();
        cfg.bucket = gcs_config.bucket_name.clone();
        cfg.root = gcs_config.prefix_in_bucket.clone();

        // If the `GOOGLE_CLOUD_STORAGE_CREDENTIAL_PATH` env var has an path, use that,
        // otherwise let opendal to try gcs well-known path.
        if let Ok(credential_path) = env::var("GOOGLE_CLOUD_STORAGE_CREDENTIAL_PATH") {
            cfg.credential_path = Some(credential_path);
        }

        let client = Operator::from_config(cfg)?
            // Setup logging for operator
            .layer(LoggingLayer::default())
            // Setup timeout for operator
            .layer(TimeoutLayer::new().with_timeout(timeout))
            // Setup retry for operator
            .layer(RetryLayer::default().with_jitter())
            .finish();

        Ok(GoogleCloudStorage { client })
    }
}

fn to_download_error(error: opendal::Error) -> DownloadError {
    use opendal::ErrorKind;
    match error.kind() {
        ErrorKind::NotFound => DownloadError::NotFound,
        ErrorKind::ConditionNotMatch => DownloadError::Unmodified,
        _ => DownloadError::Other(error.into()),
    }
}

impl RemoteStorage for GoogleCloudStorage {
    fn list_streaming(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        _cancel: &CancellationToken,
    ) -> impl Stream<Item = Result<Listing, DownloadError>> + Send {
        async_stream::stream! {
            let mut lister_builder = self.client.lister_with(prefix.map(|p| p.get_path().as_str()).unwrap_or("/"));

            if matches!(mode, ListingMode::NoDelimiter) {
                lister_builder = lister_builder.recursive(true);
            }

            if let Some(max_keys) = max_keys {
                lister_builder = lister_builder.limit(max_keys.get() as usize);
            }

            let lister = lister_builder.await.map_err(to_download_error)?;
            // FIXME: list_streaming requires to return a vector instead.
            //
            // Maybe we can refactor here to return a stream of Object instead of Listing.
            let mut lister = lister.chunks(1000);

            while let Some(entries) = lister.next().await {
                let mut result = Listing::default();
                for entry in entries {
                    let entry = entry.map_err(to_download_error)?;
                    let key = RemotePath::from_string(entry.path()).map_err(|err| DownloadError::Other(err))?;
                    if entry.metadata().mode().is_dir() {
                        result.prefixes.push(key);
                    } else {
                        result.keys.push(ListingObject {
                            key,
                            last_modified: entry.metadata().last_modified().unwrap_or_default().into(),
                            size: entry.metadata().content_length(),
                        });
                    }
                }

                yield Ok(result);
            }
        }
    }

    async fn head_object(
        &self,
        key: &RemotePath,
        _cancel: &CancellationToken,
    ) -> Result<ListingObject, DownloadError> {
        let meta = self
            .client
            .stat(key.get_path().as_str())
            .await
            .map_err(to_download_error)?;

        Ok(ListingObject {
            key: key.to_owned(),
            last_modified: meta.last_modified().unwrap_or_default().into(),
            size: meta.content_length(),
        })
    }

    async fn upload(
        &self,
        mut from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        _data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        _cancel: &CancellationToken,
    ) -> Result<()> {
        let mut fut = self.client.writer_with(to.get_path().as_str());

        if let Some(metadata) = metadata {
            fut = fut.user_metadata(metadata.0);
        }

        let writer = fut.await?;
        let mut sink = writer.into_bytes_sink();
        let mut from = pin!(from);
        sink.send_all(&mut from).await?;
        sink.close().await?;

        Ok(())
    }

    /// ## TODO
    ///
    /// - OpenDAL doesn't support returning metadata for read so far. Calling stat to get it, tracked at https://github.com/apache/opendal/issues/5425
    /// - OpenDAL doesn't support `if-none-match` on reader yet, tracked at https://github.com/apache/opendal/issues/5426
    async fn download(
        &self,
        from: &RemotePath,
        opts: &DownloadOpts,
        _cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        let meta = self
            .client
            .stat(from.get_path().as_str())
            .await
            .map_err(to_download_error)?;

        let fut = self
            .client
            .reader(from.get_path().as_str())
            .await
            .map_err(to_download_error)?;
        let stream = fut
            .into_bytes_stream((opts.byte_end, opts.byte_end))
            .await
            .map_err(to_download_error)?;

        Ok(Download {
            download_stream: Box::pin(stream),
            last_modified: meta.last_modified().unwrap_or_default().into(),
            etag: meta.etag().unwrap_or_default().into(),
            metadata: meta
                .user_metadata()
                .map(|v| StorageMetadata::new(v.clone())),
        })
    }

    async fn delete(&self, path: &RemotePath, _cancel: &CancellationToken) -> Result<()> {
        Ok(self.client.delete(path.get_path().as_ref()).await?)
    }

    async fn delete_objects(
        &self,
        paths: &[RemotePath],
        _cancel: &CancellationToken,
    ) -> Result<()> {
        Ok(self
            .client
            .remove(paths.iter().map(|p| p.get_path().to_string()).collect())
            .await?)
    }

    fn max_keys_per_delete(&self) -> usize {
        self.client
            .info()
            .full_capability()
            .batch_max_operations
            .unwrap_or(1)
    }

    async fn copy(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        _cancel: &CancellationToken,
    ) -> Result<()> {
        Ok(self
            .client
            .copy(from.get_path().as_str(), to.get_path().as_str())
            .await?)
    }

    /// TODO: We can implement via opendal's list_with(path).version(true).await;
    async fn time_travel_recover(
        &self,
        _prefix: Option<&RemotePath>,
        _timestamp: SystemTime,
        _done_if_after: SystemTime,
        _cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError> {
        Err(TimeTravelError::Unimplemented)
    }
}
