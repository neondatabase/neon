//
// Offload old WAL segments to S3 and remove them locally
// Needs `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to be set
// if no IAM bucket access is used.
//

use anyhow::{bail, Context};
use postgres_ffi::xlog_utils::*;
use remote_storage::{
    GenericRemoteStorage, RemoteStorage, RemoteStorageConfig, S3Bucket, S3Config, S3ObjectKey,
};
use std::collections::HashSet;
use std::env;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::time::SystemTime;
use tokio::fs::{self, File};
use tokio::io::BufReader;
use tokio::runtime;
use tokio::time::sleep;
use tracing::*;
use walkdir::WalkDir;

use crate::SafeKeeperConf;

pub fn thread_main(conf: SafeKeeperConf) {
    // Create a new thread pool
    //
    // FIXME: keep it single-threaded for now, make it easier to debug with gdb,
    // and we're not concerned with performance yet.
    //let runtime = runtime::Runtime::new().unwrap();
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    info!("Starting S3 offload task");

    runtime.block_on(async {
        main_loop(&conf).await.unwrap();
    });
}

async fn offload_files(
    remote_storage: &S3Bucket,
    listing: &HashSet<S3ObjectKey>,
    dir_path: &Path,
    conf: &SafeKeeperConf,
) -> anyhow::Result<u64> {
    let horizon = SystemTime::now() - conf.ttl.unwrap();
    let mut n: u64 = 0;
    for entry in WalkDir::new(dir_path) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file()
            && IsXLogFileName(entry.file_name().to_str().unwrap())
            && entry.metadata().unwrap().created().unwrap() <= horizon
        {
            let remote_path = remote_storage.remote_object_id(path)?;
            if !listing.contains(&remote_path) {
                let file = File::open(&path).await?;
                let file_length = file.metadata().await?.len() as usize;
                remote_storage
                    .upload(BufReader::new(file), file_length, &remote_path, None)
                    .await?;

                fs::remove_file(&path).await?;
                n += 1;
            }
        }
    }
    Ok(n)
}

async fn main_loop(conf: &SafeKeeperConf) -> anyhow::Result<()> {
    let remote_storage = match GenericRemoteStorage::new(
        conf.workdir.clone(),
        &RemoteStorageConfig {
            max_concurrent_syncs: NonZeroUsize::new(10).unwrap(),
            max_sync_errors: NonZeroU32::new(1).unwrap(),
            storage: remote_storage::RemoteStorageKind::AwsS3(S3Config {
                bucket_name: "zenith-testbucket".to_string(),
                bucket_region: env::var("S3_REGION").context("S3_REGION env var is not set")?,
                prefix_in_bucket: Some("walarchive/".to_string()),
                endpoint: Some(env::var("S3_ENDPOINT").context("S3_ENDPOINT env var is not set")?),
                concurrency_limit: NonZeroUsize::new(20).unwrap(),
            }),
        },
    )? {
        GenericRemoteStorage::Local(_) => {
            bail!("Unexpected: got local storage for the remote config")
        }
        GenericRemoteStorage::S3(remote_storage) => remote_storage,
    };

    loop {
        let listing = remote_storage
            .list()
            .await?
            .into_iter()
            .collect::<HashSet<_>>();
        let n = offload_files(&remote_storage, &listing, &conf.workdir, conf).await?;
        info!("Offload {n} files to S3");
        sleep(conf.ttl.unwrap()).await;
    }
}
