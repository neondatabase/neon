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
