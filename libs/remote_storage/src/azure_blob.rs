//! Azure Blob Storage wrapper

use std::{borrow::Cow, collections::HashMap, io::Cursor};

use super::REMOTE_STORAGE_PREFIX_SEPARATOR;
use anyhow::Result;
use azure_core::request_options::{Metadata, Range};
use azure_core::Header;
use azure_storage_blobs::{
    blob::operations::GetBlobBuilder,
    prelude::{BlobClient, ContainerClient},
};
use futures_util::StreamExt;
use http_types::StatusCode;
use tokio::io::AsyncRead;

use crate::{Download, DownloadError, RemotePath, RemoteStorage, StorageMetadata};

pub struct AzureBlobStorage {
    client: ContainerClient,
    prefix_in_container: Option<String>,
}

impl AzureBlobStorage {
    pub fn new() -> Result<Self> {
        todo!()
    }

    pub fn relative_path_to_name(&self, path: &RemotePath) -> String {
        assert_eq!(std::path::MAIN_SEPARATOR, REMOTE_STORAGE_PREFIX_SEPARATOR);
        let path_string = path
            .get_path()
            .as_str()
            .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR);
        match &self.prefix_in_container {
            Some(prefix) => prefix.clone() + "/" + path_string,
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
        let mut buf = Vec::new();
        while let Some(part) = response.next().await {
            let part = match part {
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

        match builder.into_future().await {
            Ok(r) => {
                let mut map = HashMap::new();

                for md in r.metadata.iter() {
                    map.insert(
                        md.name().as_str().to_string(),
                        md.value().as_str().to_string(),
                    );
                }
                Ok(StorageMetadata(map))
            }
            Err(e) => {
                return Err(if let Some(htttp_err) = e.as_http_error() {
                    match htttp_err.status() {
                        StatusCode::NotFound => DownloadError::NotFound,
                        StatusCode::BadRequest => DownloadError::BadInput(anyhow::Error::new(e)),
                        _ => DownloadError::Other(anyhow::Error::new(e)),
                    }
                } else {
                    DownloadError::Other(e.into())
                });
            }
        }
    }
}

fn to_azure_metadata(metadata: StorageMetadata) -> Metadata {
    let mut res = Metadata::new();
    for (k, v) in metadata.0.into_iter() {
        res.insert(k, v);
    }
    res
}

#[async_trait::async_trait]
impl RemoteStorage for AzureBlobStorage {
    async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        self.list_prefixes(folder).await.map_err(|err| match err {
            DownloadError::NotFound => anyhow::anyhow!("not found"), // TODO maybe return empty list?
            DownloadError::BadInput(e) | DownloadError::Other(e) => e,
        })
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
            res.extend(
                entry
                    .blobs
                    .blobs()
                    .map(|bl| self.name_to_relative_path(&bl.name)),
            );
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
        let blob_client = self.client.blob_client(self.relative_path_to_name(to));

        // TODO FIX THIS UGLY HACK and don't buffer the entire object
        // into RAM here, but use the streaming interface. For that,
        // we'd have to change the interface though...
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
        let blob_client = self.client.blob_client(self.relative_path_to_name(from));

        let metadata = self.get_metadata(&blob_client).await?;

        let mut builder = blob_client.get();

        if let Some(end_exclusive) = end_exclusive {
            builder = builder.range(Range::new(start_inclusive, end_exclusive));
        } else {
            // TODO: what to do about open ranges??
            // TODO: file upstream issue that the Range type doesn't support open ranges
            // TODO also it should be documented if the end is exclusive or inclusive.
            // https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations
            panic!("open ranges are not supported!");
        }

        self.download_for_builder(metadata, builder).await
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        let blob_client = self.client.blob_client(self.relative_path_to_name(path));

        let builder = blob_client.delete();

        match builder.into_future().await {
            Ok(_response) => Ok(()),
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }

    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        // TODO batch requests are also not supported by the SDK
        // https://github.com/Azure/azure-sdk-for-rust/issues/1068
        // https://github.com/Azure/azure-sdk-for-rust/issues/1249
        for path in paths {
            self.delete(path).await?;
        }
        Ok(())
    }
}
