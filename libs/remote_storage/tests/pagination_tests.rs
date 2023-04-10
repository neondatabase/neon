use std::collections::HashSet;
use std::env;
use std::num::{NonZeroU32, NonZeroUsize};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use remote_storage::{
    GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind, S3Config,
};
use test_context::{test_context, AsyncTestContext};
use tokio::task::JoinSet;
use tracing::{debug, error, info};

const ENABLE_REAL_S3_REMOTE_STORAGE_ENV_VAR_NAME: &str = "ENABLE_REAL_S3_REMOTE_STORAGE";

/// Tests that S3 client can list all prefixes, even if the response come paginated and requires multiple S3 queries.
/// Uses real S3 and requires [`ENABLE_REAL_S3_REMOTE_STORAGE_ENV_VAR_NAME`] and related S3 cred env vars specified.
/// See the client creation in [`create_s3_client`] for details on the required env vars.
/// If real S3 tests are disabled, the test passes, skipping any real test run: currently, there's no way to mark the test ignored in runtime with the
/// deafult test framework, see https://github.com/rust-lang/rust/issues/68007 for details.
///
/// First, the test creates a set of S3 objects with keys `/${random_prefix_part}/${base_prefix_str}/sub_prefix_${i}/blob_${i}` in [`upload_s3_data`]
/// where
/// * `random_prefix_part` is set for the entire S3 client during the S3 client creation in [`create_s3_client`], to avoid multiple test runs interference
/// * `base_prefix_str` is a common prefix to use in the client requests: we would want to ensure that the client is able to list nested prefixes inside the bucket
///
/// Then, verifies that the client does return correct prefixes when queried:
/// * with no prefix, it lists everything after its `${random_prefix_part}/` — that should be `${base_prefix_str}` value only
/// * with `${base_prefix_str}/` prefix, it lists every `sub_prefix_${i}`
///
/// With the real S3 enabled and `#[cfg(test)]` Rust configuration used, the S3 client test adds a `max-keys` param to limit the response keys.
/// This way, we are able to test the pagination implicitly, by ensuring all results are returned from the remote storage and avoid uploading too many blobs to S3,
/// since current default AWS S3 pagination limit is 1000.
/// (see https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax)
///
/// Lastly, the test attempts to clean up and remove all uploaded S3 files.
/// If any errors appear during the clean up, they get logged, but the test is not failed or stopped until clean up is finished.
#[test_context(MaybeEnabledS3)]
#[tokio::test]
async fn s3_pagination_should_work(ctx: &mut MaybeEnabledS3) -> anyhow::Result<()> {
    let ctx = match ctx {
        MaybeEnabledS3::Enabled(ctx) => ctx,
        MaybeEnabledS3::Disabled => return Ok(()),
        MaybeEnabledS3::UploadsFailed(e, _) => anyhow::bail!("S3 init failed: {e:?}"),
    };

    let test_client = Arc::clone(&ctx.client_with_excessive_pagination);
    let expected_remote_prefixes = ctx.remote_prefixes.clone();

    let base_prefix =
        RemotePath::new(Path::new(ctx.base_prefix_str)).context("common_prefix construction")?;
    let root_remote_prefixes = test_client
        .list_prefixes(None)
        .await
        .context("client list root prefixes failure")?
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(
        root_remote_prefixes, HashSet::from([base_prefix.clone()]),
        "remote storage root prefixes list mismatches with the uploads. Returned prefixes: {root_remote_prefixes:?}"
    );

    let nested_remote_prefixes = test_client
        .list_prefixes(Some(&base_prefix))
        .await
        .context("client list nested prefixes failure")?
        .into_iter()
        .collect::<HashSet<_>>();
    let remote_only_prefixes = nested_remote_prefixes
        .difference(&expected_remote_prefixes)
        .collect::<HashSet<_>>();
    let missing_uploaded_prefixes = expected_remote_prefixes
        .difference(&nested_remote_prefixes)
        .collect::<HashSet<_>>();
    assert_eq!(
        remote_only_prefixes.len() + missing_uploaded_prefixes.len(), 0,
        "remote storage nested prefixes list mismatches with the uploads. Remote only prefixes: {remote_only_prefixes:?}, missing uploaded prefixes: {missing_uploaded_prefixes:?}",
    );

    Ok(())
}

enum MaybeEnabledS3 {
    Enabled(S3WithTestBlobs),
    Disabled,
    UploadsFailed(anyhow::Error, S3WithTestBlobs),
}

struct S3WithTestBlobs {
    client_with_excessive_pagination: Arc<GenericRemoteStorage>,
    base_prefix_str: &'static str,
    remote_prefixes: HashSet<RemotePath>,
    remote_blobs: HashSet<RemotePath>,
}

#[async_trait::async_trait]
impl AsyncTestContext for MaybeEnabledS3 {
    async fn setup() -> Self {
        utils::logging::init(utils::logging::LogFormat::Test).expect("logging init failed");
        if env::var(ENABLE_REAL_S3_REMOTE_STORAGE_ENV_VAR_NAME).is_err() {
            info!(
                "`{}` env variable is not set, skipping the test",
                ENABLE_REAL_S3_REMOTE_STORAGE_ENV_VAR_NAME
            );
            return Self::Disabled;
        }

        let max_keys_in_list_response = 10;
        let upload_tasks_count = 1 + (2 * usize::try_from(max_keys_in_list_response).unwrap());

        let client_with_excessive_pagination = create_s3_client(max_keys_in_list_response)
            .context("S3 client creation")
            .expect("S3 client creation failed");

        let base_prefix_str = "test/";
        match upload_s3_data(
            &client_with_excessive_pagination,
            base_prefix_str,
            upload_tasks_count,
        )
        .await
        {
            ControlFlow::Continue(uploads) => {
                info!("Remote objects created successfully");
                Self::Enabled(S3WithTestBlobs {
                    client_with_excessive_pagination,
                    base_prefix_str,
                    remote_prefixes: uploads.prefixes,
                    remote_blobs: uploads.blobs,
                })
            }
            ControlFlow::Break(uploads) => Self::UploadsFailed(
                anyhow::anyhow!("One or multiple blobs failed to upload to S3"),
                S3WithTestBlobs {
                    client_with_excessive_pagination,
                    base_prefix_str,
                    remote_prefixes: uploads.prefixes,
                    remote_blobs: uploads.blobs,
                },
            ),
        }
    }

    async fn teardown(self) {
        match self {
            Self::Disabled => {}
            Self::Enabled(ctx) | Self::UploadsFailed(_, ctx) => {
                cleanup(&ctx.client_with_excessive_pagination, ctx.remote_blobs).await;
            }
        }
    }
}

fn create_s3_client(max_keys_per_list_response: i32) -> anyhow::Result<Arc<GenericRemoteStorage>> {
    let remote_storage_s3_bucket = env::var("REMOTE_STORAGE_S3_BUCKET")
        .context("`REMOTE_STORAGE_S3_BUCKET` env var is not set, but real S3 tests are enabled")?;
    let remote_storage_s3_region = env::var("REMOTE_STORAGE_S3_REGION")
        .context("`REMOTE_STORAGE_S3_REGION` env var is not set, but real S3 tests are enabled")?;
    let random_prefix_part = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("random s3 test prefix part calculation")?
        .as_millis();
    let remote_storage_config = RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).unwrap(),
        max_sync_errors: NonZeroU32::new(5).unwrap(),
        storage: RemoteStorageKind::AwsS3(S3Config {
            bucket_name: remote_storage_s3_bucket,
            bucket_region: remote_storage_s3_region,
            prefix_in_bucket: Some(format!("pagination_should_work_test_{random_prefix_part}/")),
            endpoint: None,
            concurrency_limit: NonZeroUsize::new(100).unwrap(),
            max_keys_per_list_response: Some(max_keys_per_list_response),
        }),
    };
    Ok(Arc::new(
        GenericRemoteStorage::from_config(&remote_storage_config).context("remote storage init")?,
    ))
}

struct Uploads {
    prefixes: HashSet<RemotePath>,
    blobs: HashSet<RemotePath>,
}

async fn upload_s3_data(
    client: &Arc<GenericRemoteStorage>,
    base_prefix_str: &'static str,
    upload_tasks_count: usize,
) -> ControlFlow<Uploads, Uploads> {
    info!("Creating {upload_tasks_count} S3 files");
    let mut upload_tasks = JoinSet::new();
    for i in 1..upload_tasks_count + 1 {
        let task_client = Arc::clone(client);
        upload_tasks.spawn(async move {
            let prefix = PathBuf::from(format!("{base_prefix_str}/sub_prefix_{i}/"));
            let blob_prefix = RemotePath::new(&prefix)
                .with_context(|| format!("{prefix:?} to RemotePath conversion"))?;
            let blob_path = blob_prefix.join(Path::new(&format!("blob_{i}")));
            debug!("Creating remote item {i} at path {blob_path:?}");

            let data = format!("remote blob data {i}").into_bytes();
            let data_len = data.len();
            task_client
                .upload(std::io::Cursor::new(data), data_len, &blob_path, None)
                .await?;

            Ok::<_, anyhow::Error>((blob_prefix, blob_path))
        });
    }

    let mut upload_tasks_failed = false;
    let mut uploaded_prefixes = HashSet::with_capacity(upload_tasks_count);
    let mut uploaded_blobs = HashSet::with_capacity(upload_tasks_count);
    while let Some(task_run_result) = upload_tasks.join_next().await {
        match task_run_result
            .context("task join failed")
            .and_then(|task_result| task_result.context("upload task failed"))
        {
            Ok((upload_prefix, upload_path)) => {
                uploaded_prefixes.insert(upload_prefix);
                uploaded_blobs.insert(upload_path);
            }
            Err(e) => {
                error!("Upload task failed: {e:?}");
                upload_tasks_failed = true;
            }
        }
    }

    let uploads = Uploads {
        prefixes: uploaded_prefixes,
        blobs: uploaded_blobs,
    };
    if upload_tasks_failed {
        ControlFlow::Break(uploads)
    } else {
        ControlFlow::Continue(uploads)
    }
}

async fn cleanup(client: &Arc<GenericRemoteStorage>, objects_to_delete: HashSet<RemotePath>) {
    info!(
        "Removing {} objects from the remote storage during cleanup",
        objects_to_delete.len()
    );
    let mut delete_tasks = JoinSet::new();
    for object_to_delete in objects_to_delete {
        let task_client = Arc::clone(client);
        delete_tasks.spawn(async move {
            debug!("Deleting remote item at path {object_to_delete:?}");
            task_client
                .delete(&object_to_delete)
                .await
                .with_context(|| format!("{object_to_delete:?} removal"))
        });
    }

    while let Some(task_run_result) = delete_tasks.join_next().await {
        match task_run_result {
            Ok(task_result) => match task_result {
                Ok(()) => {}
                Err(e) => error!("Delete task failed: {e:?}"),
            },
            Err(join_err) => error!("Delete task did not finish correctly: {join_err}"),
        }
    }
}
