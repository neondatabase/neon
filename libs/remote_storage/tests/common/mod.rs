use std::collections::HashSet;
use std::env;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use bytes::Bytes;
use camino::Utf8Path;
use futures::stream::Stream;
use once_cell::sync::OnceCell;
use remote_storage::{
    AzureConfig, Download, GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind,
};
use test_context::{test_context, AsyncTestContext};
use tokio::task::JoinSet;
use tracing::{debug, error, info};

// FIXME: copypasted from test_real_s3, can't remember how to share a module which is not compiled
// to binary
pub(crate) fn upload_stream(
    content: std::borrow::Cow<'static, [u8]>,
) -> (
    impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    usize,
) {
    use std::borrow::Cow;

    let content = match content {
        Cow::Borrowed(x) => Bytes::from_static(x),
        Cow::Owned(vec) => Bytes::from(vec),
    };
    wrap_stream(content)
}

pub(crate) fn wrap_stream(
    content: bytes::Bytes,
) -> (
    impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    usize,
) {
    let len = content.len();
    let content = futures::future::ready(Ok(content));

    (futures::stream::once(content), len)
}

// Uploads files `folder{j}/blob{i}.txt`. See test description for more details.
pub(crate) async fn upload_simple_remote_data(
    client: &Arc<GenericRemoteStorage>,
    upload_tasks_count: usize,
) -> ControlFlow<HashSet<RemotePath>, HashSet<RemotePath>> {
    info!("Creating {upload_tasks_count} remote files");
    let mut upload_tasks = JoinSet::new();
    for i in 1..upload_tasks_count + 1 {
        let task_client = Arc::clone(client);
        upload_tasks.spawn(async move {
            let blob_path = PathBuf::from(format!("folder{}/blob_{}.txt", i / 7, i));
            let blob_path = RemotePath::new(
                Utf8Path::from_path(blob_path.as_path()).expect("must be valid blob path"),
            )
            .with_context(|| format!("{blob_path:?} to RemotePath conversion"))?;
            debug!("Creating remote item {i} at path {blob_path:?}");

            let (data, len) = upload_stream(format!("remote blob data {i}").into_bytes().into());
            task_client.upload(data, len, &blob_path, None).await?;

            Ok::<_, anyhow::Error>(blob_path)
        });
    }

    let mut upload_tasks_failed = false;
    let mut uploaded_blobs = HashSet::with_capacity(upload_tasks_count);
    while let Some(task_run_result) = upload_tasks.join_next().await {
        match task_run_result
            .context("task join failed")
            .and_then(|task_result| task_result.context("upload task failed"))
        {
            Ok(upload_path) => {
                uploaded_blobs.insert(upload_path);
            }
            Err(e) => {
                error!("Upload task failed: {e:?}");
                upload_tasks_failed = true;
            }
        }
    }

    if upload_tasks_failed {
        ControlFlow::Break(uploaded_blobs)
    } else {
        ControlFlow::Continue(uploaded_blobs)
    }
}