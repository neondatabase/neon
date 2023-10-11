//! Azure Blob Storage wrapper

use anyhow::Result;
use azure_storage_blobs::prelude::ContainerClient;
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
        todo!()
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
