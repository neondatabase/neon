//! Azure Blob Storage wrapper

use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::REMOTE_STORAGE_PREFIX_SEPARATOR;
use anyhow::Result;
use azure_core::request_options::{MaxResults, Metadata, Range};
use azure_core::RetryOptions;
use azure_identity::DefaultAzureCredential;
use azure_storage::StorageCredentials;
use azure_storage_blobs::blob::CopyStatus;
use azure_storage_blobs::prelude::ClientBuilder;
use azure_storage_blobs::{blob::operations::GetBlobBuilder, prelude::ContainerClient};
use bytes::Bytes;
use futures::stream::Stream;
use futures_util::StreamExt;
use http_types::{StatusCode, Url};
use tokio::time::Instant;
use tracing::debug;

use crate::s3_bucket::RequestKind;
use crate::{
    AzureConfig, ConcurrencyLimiter, Download, DownloadError, Listing, ListingMode, RemotePath,
    RemoteStorage, StorageMetadata,
};

pub struct AzureBlobStorage {
    client: ContainerClient,
    prefix_in_container: Option<String>,
    max_keys_per_list_response: Option<NonZeroU32>,
    concurrency_limiter: ConcurrencyLimiter,
}

impl AzureBlobStorage {
    pub fn new(azure_config: &AzureConfig) -> Result<Self> {
        debug!(
            "Creating azure remote storage for azure container {}",
            azure_config.container_name
        );

        let account = env::var("AZURE_STORAGE_ACCOUNT").expect("missing AZURE_STORAGE_ACCOUNT");

        // If the `AZURE_STORAGE_ACCESS_KEY` env var has an access key, use that,
        // otherwise try the token based credentials.
        let credentials = if let Ok(access_key) = env::var("AZURE_STORAGE_ACCESS_KEY") {
            StorageCredentials::access_key(account.clone(), access_key)
        } else {
            let token_credential = DefaultAzureCredential::default();
            StorageCredentials::token_credential(Arc::new(token_credential))
        };

        // we have an outer retry
        let builder = ClientBuilder::new(account, credentials).retry(RetryOptions::none());

        let client = builder.container_client(azure_config.container_name.to_owned());

        let max_keys_per_list_response =
            if let Some(limit) = azure_config.max_keys_per_list_response {
                Some(
                    NonZeroU32::new(limit as u32)
                        .ok_or_else(|| anyhow::anyhow!("max_keys_per_list_response can't be 0"))?,
                )
            } else {
                None
            };

        Ok(AzureBlobStorage {
            client,
            prefix_in_container: azure_config.prefix_in_container.to_owned(),
            max_keys_per_list_response,
            concurrency_limiter: ConcurrencyLimiter::new(azure_config.concurrency_limit.get()),
        })
    }

    pub fn relative_path_to_name(&self, path: &RemotePath) -> String {
        assert_eq!(std::path::MAIN_SEPARATOR, REMOTE_STORAGE_PREFIX_SEPARATOR);
        let path_string = path
            .get_path()
            .as_str()
            .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR);
        match &self.prefix_in_container {
            Some(prefix) => {
                if prefix.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    prefix.clone() + path_string
                } else {
                    format!("{prefix}{REMOTE_STORAGE_PREFIX_SEPARATOR}{path_string}")
                }
            }
            None => path_string.to_string(),
        }
    }

    fn name_to_relative_path(&self, key: &str) -> RemotePath {
        let relative_path =
            match key.strip_prefix(self.prefix_in_container.as_deref().unwrap_or_default()) {
                Some(stripped) => stripped,
                // we rely on Azure to return properly prefixed paths
                // for requests with a certain prefix
                None => panic!(
                    "Key {key} does not start with container prefix {:?}",
                    self.prefix_in_container
                ),
            };
        RemotePath(
            relative_path
                .split(REMOTE_STORAGE_PREFIX_SEPARATOR)
                .collect(),
        )
    }

    async fn download_for_builder(
        &self,
        builder: GetBlobBuilder,
    ) -> Result<Download, DownloadError> {
        let mut response = builder.into_stream();

        let mut etag = None;
        let mut last_modified = None;
        let mut metadata = HashMap::new();
        // TODO give proper streaming response instead of buffering into RAM
        // https://github.com/neondatabase/neon/issues/5563

        let mut bufs = Vec::new();
        while let Some(part) = response.next().await {
            let part = part.map_err(to_download_error)?;
            let etag_str: &str = part.blob.properties.etag.as_ref();
            if etag.is_none() {
                etag = Some(etag.unwrap_or_else(|| etag_str.to_owned()));
            }
            if last_modified.is_none() {
                last_modified = Some(part.blob.properties.last_modified.into());
            }
            if let Some(blob_meta) = part.blob.metadata {
                metadata.extend(blob_meta.iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
            }
            let data = part
                .data
                .collect()
                .await
                .map_err(|e| DownloadError::Other(e.into()))?;
            bufs.push(data);
        }
        Ok(Download {
            download_stream: Box::pin(futures::stream::iter(bufs.into_iter().map(Ok))),
            etag,
            last_modified,
            metadata: Some(StorageMetadata(metadata)),
        })
    }

    async fn permit(&self, kind: RequestKind) -> tokio::sync::SemaphorePermit<'_> {
        self.concurrency_limiter
            .acquire(kind)
            .await
            .expect("semaphore is never closed")
    }
}

fn to_azure_metadata(metadata: StorageMetadata) -> Metadata {
    let mut res = Metadata::new();
    for (k, v) in metadata.0.into_iter() {
        res.insert(k, v);
    }
    res
}

fn to_download_error(error: azure_core::Error) -> DownloadError {
    if let Some(http_err) = error.as_http_error() {
        match http_err.status() {
            StatusCode::NotFound => DownloadError::NotFound,
            StatusCode::BadRequest => DownloadError::BadInput(anyhow::Error::new(error)),
            _ => DownloadError::Other(anyhow::Error::new(error)),
        }
    } else {
        DownloadError::Other(error.into())
    }
}

impl RemoteStorage for AzureBlobStorage {
    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
    ) -> anyhow::Result<Listing, DownloadError> {
        // get the passed prefix or if it is not set use prefix_in_bucket value
        let list_prefix = prefix
            .map(|p| self.relative_path_to_name(p))
            .or_else(|| self.prefix_in_container.clone())
            .map(|mut p| {
                // required to end with a separator
                // otherwise request will return only the entry of a prefix
                if matches!(mode, ListingMode::WithDelimiter)
                    && !p.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR)
                {
                    p.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                }
                p
            });

        let mut builder = self.client.list_blobs();

        if let ListingMode::WithDelimiter = mode {
            builder = builder.delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string());
        }

        if let Some(prefix) = list_prefix {
            builder = builder.prefix(Cow::from(prefix.to_owned()));
        }

        if let Some(limit) = self.max_keys_per_list_response {
            builder = builder.max_results(MaxResults::new(limit));
        }

        let mut response = builder.into_stream();
        let mut res = Listing::default();
        while let Some(l) = response.next().await {
            let entry = l.map_err(to_download_error)?;
            let prefix_iter = entry
                .blobs
                .prefixes()
                .map(|prefix| self.name_to_relative_path(&prefix.name));
            res.prefixes.extend(prefix_iter);

            let blob_iter = entry
                .blobs
                .blobs()
                .map(|k| self.name_to_relative_path(&k.name));
            res.keys.extend(blob_iter);
        }
        Ok(res)
    }

    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let _permit = self.permit(RequestKind::Put).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(to));

        let from: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>> =
            Box::pin(from);

        let from = NonSeekableStream::new(from, data_size_bytes);

        let body = azure_core::Body::SeekableStream(Box::new(from));

        let mut builder = blob_client.put_block_blob(body);

        if let Some(metadata) = metadata {
            builder = builder.metadata(to_azure_metadata(metadata));
        }

        let _response = builder.into_future().await?;

        Ok(())
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        let _permit = self.permit(RequestKind::Get).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(from));

        let builder = blob_client.get();

        self.download_for_builder(builder).await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        let _permit = self.permit(RequestKind::Get).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(from));

        let mut builder = blob_client.get();

        let range: Range = if let Some(end_exclusive) = end_exclusive {
            (start_inclusive..end_exclusive).into()
        } else {
            (start_inclusive..).into()
        };
        builder = builder.range(range);

        self.download_for_builder(builder).await
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        let _permit = self.permit(RequestKind::Delete).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(path));

        let builder = blob_client.delete();

        match builder.into_future().await {
            Ok(_response) => Ok(()),
            Err(e) => {
                if let Some(http_err) = e.as_http_error() {
                    if http_err.status() == StatusCode::NotFound {
                        return Ok(());
                    }
                }
                Err(anyhow::Error::new(e))
            }
        }
    }

    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        // Permit is already obtained by inner delete function

        // TODO batch requests are also not supported by the SDK
        // https://github.com/Azure/azure-sdk-for-rust/issues/1068
        // https://github.com/Azure/azure-sdk-for-rust/issues/1249
        for path in paths {
            self.delete(path).await?;
        }
        Ok(())
    }

    async fn copy(&self, from: &RemotePath, to: &RemotePath) -> anyhow::Result<()> {
        let _permit = self.permit(RequestKind::Copy).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(to));

        let source_url = format!(
            "{}/{}",
            self.client.url()?,
            self.relative_path_to_name(from)
        );
        let builder = blob_client.copy(Url::from_str(&source_url)?);

        let result = builder.into_future().await?;

        let mut copy_status = result.copy_status;
        let start_time = Instant::now();
        const MAX_WAIT_TIME: Duration = Duration::from_secs(60);
        loop {
            match copy_status {
                CopyStatus::Aborted => {
                    anyhow::bail!("Received abort for copy from {from} to {to}.");
                }
                CopyStatus::Failed => {
                    anyhow::bail!("Received failure response for copy from {from} to {to}.");
                }
                CopyStatus::Success => return Ok(()),
                CopyStatus::Pending => (),
            }
            // The copy is taking longer. Waiting a second and then re-trying.
            // TODO estimate time based on copy_progress and adjust time based on that
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let properties = blob_client.get_properties().into_future().await?;
            let Some(status) = properties.blob.properties.copy_status else {
                tracing::warn!("copy_status for copy is None!, from={from}, to={to}");
                return Ok(());
            };
            if start_time.elapsed() > MAX_WAIT_TIME {
                anyhow::bail!("Copy from from {from} to {to} took longer than limit MAX_WAIT_TIME={}s. copy_pogress={:?}.",
                    MAX_WAIT_TIME.as_secs_f32(),
                    properties.blob.properties.copy_progress,
                );
            }
            copy_status = status;
        }
    }
}

pin_project_lite::pin_project! {
    /// Hack to work around not being able to stream once with azure sdk.
    ///
    /// Azure sdk clones streams around with the assumption that they are like
    /// `Arc<tokio::fs::File>` (except not supporting tokio), however our streams are not like
    /// that. For example for an `index_part.json` we just have a single chunk of [`Bytes`]
    /// representing the whole serialized vec. It could be trivially cloneable and "semi-trivially"
    /// seekable, but we can also just re-try the request easier.
    #[project = NonSeekableStreamProj]
    enum NonSeekableStream<S> {
        /// A stream wrappers initial form.
        ///
        /// Mutex exists to allow moving when cloning. If the sdk changes to do less than 1
        /// clone before first request, then this must be changed.
        Initial {
            inner: std::sync::Mutex<Option<tokio_util::compat::Compat<tokio_util::io::StreamReader<S, Bytes>>>>,
            len: usize,
        },
        /// The actually readable variant, produced by cloning the Initial variant.
        ///
        /// The sdk currently always clones once, even without retry policy.
        Actual {
            #[pin]
            inner: tokio_util::compat::Compat<tokio_util::io::StreamReader<S, Bytes>>,
            len: usize,
            read_any: bool,
        },
        /// Most likely unneeded, but left to make life easier, in case more clones are added.
        Cloned {
            len_was: usize,
        }
    }
}

impl<S> NonSeekableStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    fn new(inner: S, len: usize) -> NonSeekableStream<S> {
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let inner = tokio_util::io::StreamReader::new(inner).compat();
        let inner = Some(inner);
        let inner = std::sync::Mutex::new(inner);
        NonSeekableStream::Initial { inner, len }
    }
}

impl<S> std::fmt::Debug for NonSeekableStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial { len, .. } => f.debug_struct("Initial").field("len", len).finish(),
            Self::Actual { len, .. } => f.debug_struct("Actual").field("len", len).finish(),
            Self::Cloned { len_was, .. } => f.debug_struct("Cloned").field("len", len_was).finish(),
        }
    }
}

impl<S> futures::io::AsyncRead for NonSeekableStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>>,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.project() {
            NonSeekableStreamProj::Actual {
                inner, read_any, ..
            } => {
                *read_any = true;
                inner.poll_read(cx, buf)
            }
            // NonSeekableStream::Initial does not support reading because it is just much easier
            // to have the mutex in place where one does not poll the contents, or that's how it
            // seemed originally. If there is a version upgrade which changes the cloning, then
            // that support needs to be hacked in.
            //
            // including {self:?} into the message would be useful, but unsure how to unproject.
            _ => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "cloned or initial values cannot be read",
            ))),
        }
    }
}

impl<S> Clone for NonSeekableStream<S> {
    /// Weird clone implementation exists to support the sdk doing cloning before issuing the first
    /// request, see type documentation.
    fn clone(&self) -> Self {
        use NonSeekableStream::*;

        match self {
            Initial { inner, len } => {
                if let Some(inner) = inner.lock().unwrap().take() {
                    Actual {
                        inner,
                        len: *len,
                        read_any: false,
                    }
                } else {
                    Self::Cloned { len_was: *len }
                }
            }
            Actual { len, .. } => Cloned { len_was: *len },
            Cloned { len_was } => Cloned { len_was: *len_was },
        }
    }
}

#[async_trait::async_trait]
impl<S> azure_core::SeekableStream for NonSeekableStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin + Send + Sync + 'static,
{
    async fn reset(&mut self) -> azure_core::error::Result<()> {
        use NonSeekableStream::*;

        let msg = match self {
            Initial { inner, .. } => {
                if inner.get_mut().unwrap().is_some() {
                    return Ok(());
                } else {
                    "reset after first clone is not supported"
                }
            }
            Actual { read_any, .. } if !*read_any => return Ok(()),
            Actual { .. } => "reset after reading is not supported",
            Cloned { .. } => "reset after second clone is not supported",
        };
        Err(azure_core::error::Error::new(
            azure_core::error::ErrorKind::Io,
            std::io::Error::new(std::io::ErrorKind::Other, msg),
        ))
    }

    // Note: it is not documented if this should be the total or remaining length, total passes the
    // tests.
    fn len(&self) -> usize {
        use NonSeekableStream::*;
        match self {
            Initial { len, .. } => *len,
            Actual { len, .. } => *len,
            Cloned { len_was, .. } => *len_was,
        }
    }
}
