//! This module provides a wrapper around a real RemoteStorage implementation that
//! causes the first N attempts at each upload or download operatio to fail. For
//! testing purposes.
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::{Download, DownloadError, RemotePath, RemoteStorage, StorageMetadata};

pub struct UnreliableWrapper {
    inner: crate::GenericRemoteStorage,

    // This many attempts of each operation will fail, then we let it succeed.
    attempts_to_fail: u64,

    // Tracks how many failed attempts of each operation has been made.
    attempts: Mutex<HashMap<RemoteOp, u64>>,
}

/// Used to identify retries of different unique operation.
#[derive(Debug, Hash, Eq, PartialEq)]
enum RemoteOp {
    ListPrefixes(Option<RemotePath>),
    Upload(RemotePath),
    Download(RemotePath),
    Delete(RemotePath),
    DeleteObjects(Vec<RemotePath>),
}

impl UnreliableWrapper {
    pub fn new(inner: crate::GenericRemoteStorage, attempts_to_fail: u64) -> Self {
        assert!(attempts_to_fail > 0);
        UnreliableWrapper {
            inner,
            attempts_to_fail,
            attempts: Mutex::new(HashMap::new()),
        }
    }

    ///
    /// Common functionality for all operations.
    ///
    /// On the first attempts of this operation, return an error. After 'attempts_to_fail'
    /// attempts, let the operation go ahead, and clear the counter.
    ///
    fn attempt(&self, op: RemoteOp) -> Result<u64, DownloadError> {
        let mut attempts = self.attempts.lock().unwrap();

        match attempts.entry(op) {
            Entry::Occupied(mut e) => {
                let attempts_before_this = {
                    let p = e.get_mut();
                    *p += 1;
                    *p
                };

                if attempts_before_this >= self.attempts_to_fail {
                    // let it succeed
                    e.remove();
                    Ok(attempts_before_this)
                } else {
                    let error =
                        anyhow::anyhow!("simulated failure of remote operation {:?}", e.key());
                    Err(DownloadError::Other(error))
                }
            }
            Entry::Vacant(e) => {
                let error = anyhow::anyhow!("simulated failure of remote operation {:?}", e.key());
                e.insert(1);
                Err(DownloadError::Other(error))
            }
        }
    }

    async fn delete_inner(&self, path: &RemotePath, attempt: bool) -> anyhow::Result<()> {
        if attempt {
            self.attempt(RemoteOp::Delete(path.clone()))?;
        }
        self.inner.delete(path).await
    }
}

#[async_trait::async_trait]
impl RemoteStorage for UnreliableWrapper {
    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        self.attempt(RemoteOp::ListPrefixes(prefix.cloned()))?;
        self.inner.list_prefixes(prefix).await
    }

    async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        self.attempt(RemoteOp::ListPrefixes(folder.cloned()))?;
        self.inner.list_files(folder).await
    }

    async fn upload(
        &self,
        data: impl tokio::io::AsyncRead + Unpin + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        self.attempt(RemoteOp::Upload(to.clone()))?;
        self.inner.upload(data, data_size_bytes, to, metadata).await
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        self.attempt(RemoteOp::Download(from.clone()))?;
        self.inner.download(from).await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        // Note: We treat any download_byte_range as an "attempt" of the same
        // operation. We don't pay attention to the ranges. That's good enough
        // for now.
        self.attempt(RemoteOp::Download(from.clone()))?;
        self.inner
            .download_byte_range(from, start_inclusive, end_exclusive)
            .await
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        self.delete_inner(path, true).await
    }

    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        self.attempt(RemoteOp::DeleteObjects(paths.to_vec()))?;
        let mut error_counter = 0;
        for path in paths {
            // Dont record attempt because it was already recorded above
            if (self.delete_inner(path, false).await).is_err() {
                error_counter += 1;
            }
        }
        if error_counter > 0 {
            return Err(anyhow::anyhow!(
                "failed to delete {} objects",
                error_counter
            ));
        }
        Ok(())
    }
}
