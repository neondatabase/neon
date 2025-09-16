#![allow(dead_code)]
#![allow(unused)]

mod common;

use crate::common::{download_to_vec, upload_stream};
use anyhow::Context;
use camino::Utf8Path;
use futures::StreamExt;
use futures::stream::Stream;
use remote_storage::{
    DownloadKind, DownloadOpts, GCSConfig, GenericRemoteStorage, ListingMode, RemotePath,
    RemoteStorageConfig, RemoteStorageKind, StorageMetadata,
};
use std::collections::HashMap;
#[path = "common/tests.rs"]
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::io::Cursor;
use std::ops::Bound;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use test_context::{AsyncTestContext, test_context};
use tokio_util::sync::CancellationToken;
use utils::backoff;

// A minimal working GCS client I can pass around in async context

const BASE_PREFIX: &str = "test";

async fn create_gcs_client() -> anyhow::Result<Arc<GenericRemoteStorage>> {
    let bucket_name = std::env::var("GCS_TEST_BUCKET").expect("GCS_TEST_BUCKET must be set");
    let gcs_config = GCSConfig {
        bucket_name,
        prefix_in_bucket: Some("testing-path/".into()),
        max_keys_per_list_response: Some(100),
        concurrency_limit: std::num::NonZero::new(100).unwrap(),
    };

    let remote_storage_config = RemoteStorageConfig {
        storage: RemoteStorageKind::GCS(gcs_config),
        timeout: Duration::from_secs(120),
        small_timeout: std::time::Duration::from_secs(120),
    };
    Ok(Arc::new(
        GenericRemoteStorage::from_config(&remote_storage_config)
            .await
            .context("remote storage init")?,
    ))
}

struct EnabledGCS {
    client: Arc<GenericRemoteStorage>,
    base_prefix: &'static str,
}

impl EnabledGCS {
    async fn setup() -> Self {
        let client = create_gcs_client()
            .await
            .context("gcs client creation")
            .expect("gcs client creation failed");
        EnabledGCS {
            client,
            base_prefix: BASE_PREFIX,
        }
    }
}

impl AsyncTestContext for EnabledGCS {
    async fn setup() -> Self {
        Self::setup().await
    }
}

#[test_context(EnabledGCS)]
#[tokio::test]
async fn gcs_test_suite(ctx: &mut EnabledGCS) -> anyhow::Result<()> {
    // ------------------------------------------------
    // --- `time_travel_recover`, showcasing `upload`, `delete_objects`, `copy`
    // ------------------------------------------------

    // Our test depends on discrepancies in the clock between S3 and the environment the tests
    // run in. Therefore, wait a little bit before and after. The alternative would be
    // to take the time from S3 response headers.
    const WAIT_TIME: Duration = Duration::from_millis(3_000);

    async fn retry<T, O, F, E>(op: O) -> Result<T, E>
    where
        E: Display + Debug + 'static,
        O: FnMut() -> F,
        F: Future<Output = Result<T, E>>,
    {
        let warn_threshold = 3;
        let max_retries = 10;
        backoff::retry(
            op,
            |_e| false,
            warn_threshold,
            max_retries,
            "test retry",
            &CancellationToken::new(),
        )
        .await
        .expect("never cancelled")
    }

    async fn time_point() -> SystemTime {
        tokio::time::sleep(WAIT_TIME).await;
        let ret = SystemTime::now();
        tokio::time::sleep(WAIT_TIME).await;
        ret
    }

    async fn list_files(
        client: &Arc<GenericRemoteStorage>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<HashSet<RemotePath>> {
        Ok(
            retry(|| client.list(None, ListingMode::NoDelimiter, None, cancel))
                .await
                .context("list root files failure")?
                .keys
                .into_iter()
                .map(|o| o.key)
                .collect::<HashSet<_>>(),
        )
    }

    let cancel = CancellationToken::new();

    let path1 = RemotePath::new(Utf8Path::new(format!("{}/path1", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    let path2 = RemotePath::new(Utf8Path::new(format!("{}/path2", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    let path3 = RemotePath::new(Utf8Path::new(format!("{}/path3", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    // ---------------- t0 ---------------
    // Upload 'path1'
    retry(|| {
        let (data, len) = upload_stream("remote blob data1".as_bytes().into());
        ctx.client.upload(data, len, &path1, None, &cancel)
    })
    .await?;
    let t0_files = list_files(&ctx.client, &cancel).await?;
    let t0 = time_point().await;

    // Show 'path1'
    println!("at t0: {t0_files:?}");

    // Upload 'path2'
    let old_data = "remote blob data2";
    retry(|| {
        let (data, len) = upload_stream(old_data.as_bytes().into());
        ctx.client.upload(data, len, &path2, None, &cancel)
    })
    .await?;

    // ---------------- t1 ---------------
    // Show 'path1' and 'path2'
    let t1_files = list_files(&ctx.client, &cancel).await?;
    let t1 = time_point().await;
    println!("at t1: {t1_files:?}");

    {
        let opts = DownloadOpts::default();
        let dl = retry(|| ctx.client.download(&path2, &opts, &cancel)).await?;
        let last_modified = dl.last_modified;
        let half_wt = WAIT_TIME.mul_f32(0.5);
        let t0_hwt = t0 + half_wt;
        let t1_hwt = t1 - half_wt;
        if !(t0_hwt..=t1_hwt).contains(&last_modified) {
            panic!(
                "last_modified={last_modified:?} is not between t0_hwt={t0_hwt:?} and t1_hwt={t1_hwt:?}. \
                This likely means a large lock discrepancy between S3 and the local clock."
            );
        }
    }

    // Upload 'path3'
    retry(|| {
        let (data, len) = upload_stream("remote blob data3".as_bytes().into());
        ctx.client.upload(data, len, &path3, None, &cancel)
    })
    .await?;

    // Overwrite 'path2'
    let new_data = "new remote blob data2";
    retry(|| {
        let (data, len) = upload_stream(new_data.as_bytes().into());
        ctx.client.upload(data, len, &path2, None, &cancel)
    })
    .await?;

    // Delete 'path1'
    retry(|| ctx.client.delete(&path1, &cancel)).await?;

    // Show 'path2' and `path3`
    let t2_files = list_files(&ctx.client, &cancel).await?;
    let t2 = time_point().await;
    println!("at t2: {t2_files:?}");

    // No changes after recovery to t2 (no-op)
    let t_final = time_point().await;
    ctx.client
        .time_travel_recover(None, t2, t_final, &cancel, None)
        .await?;
    let t2_files_recovered = list_files(&ctx.client, &cancel).await?;
    println!("after recovery to t2: {t2_files_recovered:?}");

    assert_eq!(t2_files, t2_files_recovered);
    let path2_recovered_t2 = download_to_vec(
        ctx.client
            .download(&path2, &DownloadOpts::default(), &cancel)
            .await?,
    )
    .await?;
    assert_eq!(path2_recovered_t2, new_data.as_bytes());

    // after recovery to t1: path1 is back, path2 has the old content
    let t_final = time_point().await;
    ctx.client
        .time_travel_recover(None, t1, t_final, &cancel, None)
        .await?;
    let t1_files_recovered = list_files(&ctx.client, &cancel).await?;
    println!("after recovery to t1: {t1_files_recovered:?}");
    assert_eq!(t1_files, t1_files_recovered);
    let path2_recovered_t1 = download_to_vec(
        ctx.client
            .download(&path2, &DownloadOpts::default(), &cancel)
            .await?,
    )
    .await?;
    assert_eq!(path2_recovered_t1, old_data.as_bytes());

    // after recovery to t0: everything is gone except for path1
    let t_final = time_point().await;
    ctx.client
        .time_travel_recover(None, t0, t_final, &cancel, None)
        .await?;
    let t0_files_recovered = list_files(&ctx.client, &cancel).await?;
    println!("after recovery to t0: {t0_files_recovered:?}");
    assert_eq!(t0_files, t0_files_recovered);

    // cleanup
    let paths = &[path1, path2, path3];
    retry(|| ctx.client.delete_objects(paths, &cancel)).await?;

    Ok(())
}
