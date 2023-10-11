//! Azure Blob Storage wrapper

use std::borrow::Cow;

use anyhow::Result;
use azure_storage_blobs::prelude::ContainerClient;
use futures_util::StreamExt;
use http_types::StatusCode;
use tokio::io::AsyncRead;

use crate::{Download, DownloadError, RemotePath, RemoteStorage, StorageMetadata};

pub struct AzureBlobStorage {
    client: ContainerClient,
}

impl AzureBlobStorage {
    pub fn new() -> Result<Self> {
        todo!()
    }
}

#[async_trait::async_trait]
impl RemoteStorage for AzureBlobStorage {
    async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        todo!()
    }

    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        let prefix = prefix.map(|p| Cow::from(p.to_string()));
        let mut builder = self.client.list_blobs();
        if let Some(prefix) = prefix {
            builder = builder.prefix(prefix);
        }

        let mut response = builder.into_stream();
        let mut res = Vec::new();
        while let Some(l) = response.next().await {
            let entry = match l {
                Ok(l) => l,
                Err(e) => {
                    return Err(if let Some(htttp_err) = e.as_http_error() {
                        match htttp_err.status() {
                            StatusCode::NotFound => DownloadError::NotFound,
                            StatusCode::BadRequest => {
                                DownloadError::BadInput(anyhow::Error::new(e))
                            }
                            _ => DownloadError::Other(anyhow::Error::new(e)),
                        }
                    } else {
                        DownloadError::Other(e.into())
                    });
                }
            };
            let paths_res = entry
                .blobs
                .blobs()
                .map(|bl| RemotePath::from_string(&bl.name));
            itertools::process_results(paths_res, |paths| res.extend(paths))
                .map_err(DownloadError::Other)?;
        }
        Ok(res)
    }

    async fn upload(
        &self,
        from: impl AsyncRead + Unpin + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        todo!()
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        todo!()
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        todo!()
    }
}
