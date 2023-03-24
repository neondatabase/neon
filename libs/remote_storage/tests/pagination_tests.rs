use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;

use anyhow::Context;
use remote_storage::{
    GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind, S3Config,
};

#[tokio::test]
async fn pagination_should_work() -> anyhow::Result<()> {
    utils::logging::init(utils::logging::LogFormat::Test)?;

    let remote_storage_config = RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).unwrap(),
        max_sync_errors: NonZeroU32::new(5).unwrap(),
        storage: RemoteStorageKind::AwsS3(S3Config {
            // TODO kb
            bucket_name: "bucket_name".to_string(),
            bucket_region: "bucket_region".to_string(),
            prefix_in_bucket: Some("prefix_in_bucket".to_string()),
            endpoint: Some("endpoint".to_string()),
            concurrency_limit: NonZeroUsize::new(100).unwrap(),
        }),
    };
    let client =
        GenericRemoteStorage::from_config(&remote_storage_config).context("remote storage init")?;

    const REMOTE_ITEMS_COUNT: usize = 1135;
    let mut expected_remote_prefixes = HashSet::with_capacity(REMOTE_ITEMS_COUNT);
    let common_prefix_str = "test/";
    let common_prefix =
        RemotePath::new(Path::new(common_prefix_str)).context("common_prefix construction")?;
    for i in 0..REMOTE_ITEMS_COUNT {
        let n = i + 1;
        dbg!(n);
        let test_remote_path = RemotePath::new(Path::new(&format!(
            "{common_prefix_str}/sub_prefix_{n}/blob_{n}",
        )))
        .context("test_remote_path construction")?;
        let data = format!("remote blob data {n}").into_bytes();
        let data_len = data.len();
        client
            .upload(
                Box::new(std::io::Cursor::new(data)),
                data_len,
                &test_remote_path,
                None,
            )
            .await?;
        expected_remote_prefixes.insert(
            RemotePath::new(Path::new(&format!("{common_prefix_str}/sub_prefix_{n}/",)))
                .context("expected remote prefix construction")?,
        );
    }

    let remote_elements = client
        .list_prefixes(Some(&common_prefix))
        .await
        .context("client list elements failure")?
        .into_iter()
        .collect::<HashSet<_>>();

    let remote_only_elements = remote_elements
        .difference(&expected_remote_prefixes)
        .collect::<HashSet<_>>();

    let missing_uploaded_elements = expected_remote_prefixes
        .difference(&remote_elements)
        .collect::<HashSet<_>>();

    assert!(
        remote_only_elements.len() + missing_uploaded_elements.len() == 0,
        "remote storage list mismatches with the uploads. Remote only elements: {remote_only_elements:?}, missing uploaded elements: {missing_uploaded_elements:?}",
    );

    Ok(())
}
