//
// Offload old WAL segments to S3 and remove them locally
//

use anyhow::Context;
use postgres_ffi::xlog_utils::*;
use rusoto_core::credential::StaticProvider;
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, S3};
use std::collections::HashSet;
use std::env;
use std::path::Path;
use std::time::SystemTime;
use tokio::fs::{self, File};
use tokio::runtime;
use tokio::time::sleep;
use tokio_util::io::ReaderStream;
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
    client: &S3Client,
    bucket_name: &str,
    listing: &HashSet<String>,
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
            let relpath = path.strip_prefix(&conf.workdir).unwrap();
            let s3path = String::from("walarchive/") + relpath.to_str().unwrap();
            if !listing.contains(&s3path) {
                let file = File::open(&path).await?;
                client
                    .put_object(PutObjectRequest {
                        body: Some(StreamingBody::new(ReaderStream::new(file))),
                        bucket: bucket_name.to_string(),
                        key: s3path,
                        ..PutObjectRequest::default()
                    })
                    .await?;

                fs::remove_file(&path).await?;
                n += 1;
            }
        }
    }
    Ok(n)
}

async fn main_loop(conf: &SafeKeeperConf) -> anyhow::Result<()> {
    let region = Region::Custom {
        name: env::var("S3_REGION").context("S3_REGION env var is not set")?,
        endpoint: env::var("S3_ENDPOINT").context("S3_ENDPOINT env var is not set")?,
    };

    let client = S3Client::new_with(
        HttpClient::new().context("Failed to create S3 http client")?,
        StaticProvider::new_minimal(
            env::var("S3_ACCESSKEY").context("S3_ACCESSKEY env var is not set")?,
            env::var("S3_SECRET").context("S3_SECRET env var is not set")?,
        ),
        region,
    );

    let bucket_name = "zenith-testbucket";

    loop {
        let listing = gather_wal_entries(&client, bucket_name).await?;
        let n = offload_files(&client, bucket_name, &listing, &conf.workdir, conf).await?;
        info!("Offload {} files to S3", n);
        sleep(conf.ttl.unwrap()).await;
    }
}

async fn gather_wal_entries(
    client: &S3Client,
    bucket_name: &str,
) -> anyhow::Result<HashSet<String>> {
    let mut document_keys = HashSet::new();

    let mut continuation_token = None::<String>;
    loop {
        let response = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: bucket_name.to_string(),
                prefix: Some("walarchive/".to_string()),
                continuation_token,
                ..ListObjectsV2Request::default()
            })
            .await?;
        document_keys.extend(
            response
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| o.key),
        );

        continuation_token = response.continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }
    Ok(document_keys)
}
