#![allow(dead_code)]
#![allow(unused)]

use anyhow::Context;
use futures::StreamExt;
use futures::stream::Stream;
use remote_storage::{
    DownloadKind, DownloadOpts, GCSConfig, GenericRemoteStorage, RemotePath, RemoteStorageConfig,
    RemoteStorageKind, StorageMetadata,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Bound;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use test_context::{AsyncTestContext, test_context};
use tokio_util::sync::CancellationToken;

// A minimal working GCS client I can pass around in async context

async fn create_gcs_client() -> anyhow::Result<Arc<GenericRemoteStorage>> {
    let bucket_name = std::env::var("GCS_TEST_BUCKET").expect("GCS_TEST_BUCKET must be set");
    let gcs_config = GCSConfig {
        bucket_name,
        prefix_in_bucket: Some("pageserver/".into()),
        max_keys_per_list_response: Some(100),
        concurrency_limit: std::num::NonZero::new(100).unwrap(),
    };

    let remote_storage_config = RemoteStorageConfig {
        storage: RemoteStorageKind::GCS(gcs_config),
        timeout: Duration::from_secs(120),
        small_timeout: std::time::Duration::from_secs(30),
    };
    Ok(Arc::new(
        GenericRemoteStorage::from_config(&remote_storage_config)
            .await
            .context("remote storage init")?,
    ))
}

struct EnabledGCS {
    client: Arc<GenericRemoteStorage>,
}

impl EnabledGCS {
    async fn setup() -> Self {
        let client = create_gcs_client()
            .await
            .context("gcs client creation")
            .expect("gcs client creation failed");
        EnabledGCS { client }
    }
}

impl AsyncTestContext for EnabledGCS {
    async fn setup() -> Self {
        Self::setup().await
    }
}

#[test_context(EnabledGCS)]
#[tokio::test]
async fn gcs_test_suite_streaming_upload_download_delete_large_file(
    ctx: &mut EnabledGCS,
) -> anyhow::Result<()> {
    let gcs = &ctx.client;

    let source_file = std::io::Cursor::new(vec![0; 256000000]);
    let file_size = 256000000 as usize;
    let reader = tokio_util::io::ReaderStream::with_capacity(source_file, file_size);
    // shared function arguments
    let remote_path = RemotePath::from_string("large_file.dat")?;
    let cancel = CancellationToken::new();

    // order matters and that's okay for now
    let res = gcs
        .upload(
            reader,
            file_size,
            &remote_path,
            Some(StorageMetadata::from([(
                "name",
                "pageserver/large_file.dat",
            )])),
            &cancel,
        )
        .await?;

    let opts = DownloadOpts {
        etag: None,
        byte_start: Bound::Unbounded,
        byte_end: Bound::Unbounded,
        version_id: None,
        kind: DownloadKind::Small,
    };
    let res = gcs.download(&remote_path, &opts, &cancel).await?;
    let mut stream = std::pin::pin!(res.download_stream);
    while let Some(item) = stream.next().await {
        let bytes = item?;
        if !bytes.len() == 256 {
            panic!("failed")
        }
    }

    let paths = [remote_path];
    gcs.delete_objects(&paths, &cancel).await?;

    Ok(())
}

#[test_context(EnabledGCS)]
#[tokio::test]
async fn gcs_test_multipart_upload_manifest_without_name_set(
    ctx: &mut EnabledGCS,
) -> anyhow::Result<()> {
    let gcs = &ctx.client;

    // Failed to upload data of length 58 to storage path RemotePath("tenants/99336152a31c64b41034e4e904629ce9-0102/tenant-manifest-00000001.json"
    let source_file = std::io::Cursor::new(vec![0; 256]);
    let file_size = 256 as usize;
    let reader = tokio_util::io::ReaderStream::with_capacity(source_file, file_size);

    // shared function arguments
    let path = "tenants/99336152a31c64b41034e4e904629ce9-0102/tenant-manifest-00000001.json";
    let remote_path = RemotePath::from_string(path)?;
    let cancel = CancellationToken::new();

    // order matters and that's okay for now
    let res = gcs
        .upload(reader, file_size, &remote_path, None, &cancel)
        .await?;

    let paths = [remote_path];
    gcs.delete_objects(&paths, &cancel).await?;

    Ok(())
}

#[test_context(EnabledGCS)]
#[tokio::test]
async fn gcs_test_download_manifest_without_name_set(ctx: &mut EnabledGCS) -> anyhow::Result<()> {
    let gcs = &ctx.client;

    // Failed to upload data of length 58 to storage path RemotePath("tenants/99336152a31c64b41034e4e904629ce9-0102/tenant-manifest-00000001.json"
    let source_file = std::io::Cursor::new(vec![0; 256]);
    let file_size = 256 as usize;
    let reader = tokio_util::io::ReaderStream::with_capacity(source_file, file_size);

    // shared function arguments
    let path = "tenants/99336152a31c64b41034e4e904629ce9-0102/tenant-manifest-00000001.json";
    let remote_path = RemotePath::from_string(path)?;
    let cancel = CancellationToken::new();

    let opts = DownloadOpts {
        etag: None,
        byte_start: Bound::Unbounded,
        byte_end: Bound::Unbounded,
        version_id: None,
        kind: DownloadKind::Small,
    };
    let res = gcs.download(&remote_path, &opts, &cancel).await?;
    let mut stream = std::pin::pin!(res.download_stream);
    while let Some(item) = stream.next().await {
        let bytes = item?;
        if !bytes.len() == 256 {
            panic!("failed")
        }
    }
    Ok(())
}

#[test_context(EnabledGCS)]
#[tokio::test]
async fn gcs_test_delete_key_not_found_does_not_error(ctx: &mut EnabledGCS) -> anyhow::Result<()> {
    let gcs = &ctx.client;

    let key = "neon/safekeeper/99336152a31c64b41034e4e904629ce9/814ce0bd2ae452e11575402e8296b64d/000000010000000000000001_0_00000000014EEC40_00000000014EEC40_sk1.partial";
    let remote_path = RemotePath::from_string(key)?;
    let cancel = CancellationToken::new();

    let paths = [remote_path];
    gcs.delete_objects(&paths, &cancel).await?;
    // Do it again just in case it was there.
    gcs.delete_objects(&paths, &cancel).await?;

    Ok(())
}
