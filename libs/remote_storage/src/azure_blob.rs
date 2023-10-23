//! Azure Blob Storage wrapper

use std::env;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::{borrow::Cow, collections::HashMap, io::Cursor};

use super::REMOTE_STORAGE_PREFIX_SEPARATOR;
use anyhow::Result;
use azure_core::request_options::{MaxResults, Metadata, Range};
use azure_core::Header;
use azure_identity::DefaultAzureCredential;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::ClientBuilder;
use azure_storage_blobs::{
    blob::operations::GetBlobBuilder,
    prelude::{BlobClient, ContainerClient},
};
use futures_util::StreamExt;
use http_types::StatusCode;
use tokio::io::AsyncRead;
use tracing::debug;

use crate::s3_bucket::RequestKind;
use crate::{
    AzureConfig, ConcurrencyLimiter, Download, DownloadError, RemotePath, RemoteStorage,
    StorageMetadata,
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

        let builder = ClientBuilder::new(account, credentials);

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
        metadata: StorageMetadata,
        builder: GetBlobBuilder,
    ) -> Result<Download, DownloadError> {
        let mut response = builder.into_stream();

        // TODO give proper streaming response instead of buffering into RAM
        // https://github.com/neondatabase/neon/issues/5563
        let mut buf = Vec::new();
        while let Some(part) = response.next().await {
            let part = part.map_err(to_download_error)?;
            let data = part
                .data
                .collect()
                .await
                .map_err(|e| DownloadError::Other(e.into()))?;
            buf.extend_from_slice(&data.slice(..));
        }
        Ok(Download {
            download_stream: Box::pin(Cursor::new(buf)),
            metadata: Some(metadata),
        })
    }
    // TODO get rid of this function once we have metadata included in the response
    // https://github.com/Azure/azure-sdk-for-rust/issues/1439
    async fn get_metadata(
        &self,
        blob_client: &BlobClient,
    ) -> Result<StorageMetadata, DownloadError> {
        let builder = blob_client.get_metadata();

        let response = builder.into_future().await.map_err(to_download_error)?;
        let mut map = HashMap::new();

        for md in response.metadata.iter() {
            map.insert(
                md.name().as_str().to_string(),
                md.value().as_str().to_string(),
            );
        }
        Ok(StorageMetadata(map))
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

#[async_trait::async_trait]
impl RemoteStorage for AzureBlobStorage {
    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        // get the passed prefix or if it is not set use prefix_in_bucket value
        let list_prefix = prefix
            .map(|p| self.relative_path_to_name(p))
            .or_else(|| self.prefix_in_container.clone())
            .map(|mut p| {
                // required to end with a separator
                // otherwise request will return only the entry of a prefix
                if !p.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    p.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                }
                p
            });

        let mut builder = self
            .client
            .list_blobs()
            .delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string());

        if let Some(prefix) = list_prefix {
            builder = builder.prefix(Cow::from(prefix.to_owned()));
        }

        if let Some(limit) = self.max_keys_per_list_response {
            builder = builder.max_results(MaxResults::new(limit));
        }

        let mut response = builder.into_stream();
        let mut res = Vec::new();
        while let Some(entry) = response.next().await {
            let entry = entry.map_err(to_download_error)?;
            let name_iter = entry
                .blobs
                .prefixes()
                .map(|prefix| self.name_to_relative_path(&prefix.name));
            res.extend(name_iter);
        }
        Ok(res)
    }

    async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        let folder_name = folder
            .map(|p| self.relative_path_to_name(p))
            .or_else(|| self.prefix_in_container.clone());

        let mut builder = self.client.list_blobs();

        if let Some(folder_name) = folder_name {
            builder = builder.prefix(Cow::from(folder_name.to_owned()));
        }

        if let Some(limit) = self.max_keys_per_list_response {
            builder = builder.max_results(MaxResults::new(limit));
        }

        let mut response = builder.into_stream();
        let mut res = Vec::new();
        while let Some(l) = response.next().await {
            let entry = l.map_err(anyhow::Error::new)?;
            let name_iter = entry
                .blobs
                .blobs()
                .map(|bl| self.name_to_relative_path(&bl.name));
            res.extend(name_iter);
        }
        Ok(res)
    }

    async fn upload(
        &self,
        mut from: impl AsyncRead + Unpin + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let _permit = self.permit(RequestKind::Put).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(to));

        // TODO FIX THIS UGLY HACK and don't buffer the entire object
        // into RAM here, but use the streaming interface. For that,
        // we'd have to change the interface though...
        // https://github.com/neondatabase/neon/issues/5563
        let mut buf = Vec::with_capacity(data_size_bytes);
        tokio::io::copy(&mut from, &mut buf).await?;
        let body = azure_core::Body::Bytes(buf.into());

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

        let metadata = self.get_metadata(&blob_client).await?;

        let builder = blob_client.get();

        self.download_for_builder(metadata, builder).await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        let _permit = self.permit(RequestKind::Get).await;
        let blob_client = self.client.blob_client(self.relative_path_to_name(from));

        let metadata = self.get_metadata(&blob_client).await?;

        let mut builder = blob_client.get();

        if let Some(end_exclusive) = end_exclusive {
            builder = builder.range(Range::new(start_inclusive, end_exclusive));
        } else {
            // Open ranges are not supported by the SDK so we work around
            // by setting the upper limit extremely high (but high enough
            // to still be representable by signed 64 bit integers).
            // TODO remove workaround once the SDK adds open range support
            // https://github.com/Azure/azure-sdk-for-rust/issues/1438
            let end_exclusive = u64::MAX / 4;
            builder = builder.range(Range::new(start_inclusive, end_exclusive));
        }

        self.download_for_builder(metadata, builder).await
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
}
