//
// Offload old WAL segments to S3 and remove them locally
//

use anyhow::Result;
use log::*;
use postgres_ffi::xlog_utils::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::Path;
use std::time::SystemTime;
use tokio::runtime;
use tokio::time::sleep;
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
    bucket: &Bucket,
    listing: &HashSet<String>,
    dir_path: &Path,
    conf: &SafeKeeperConf,
) -> Result<u64> {
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
                let mut file = File::open(&path)?;
                let mut content = Vec::new();
                file.read_to_end(&mut content)?;
                bucket.put_object(s3path, &content).await?;

                fs::remove_file(&path)?;
                n += 1;
            }
        }
    }
    Ok(n)
}

async fn main_loop(conf: &SafeKeeperConf) -> Result<()> {
    let region = Region::Custom {
        region: env::var("S3_REGION").unwrap(),
        endpoint: env::var("S3_ENDPOINT").unwrap(),
    };
    let credentials = Credentials::new(
        Some(&env::var("S3_ACCESSKEY").unwrap()),
        Some(&env::var("S3_SECRET").unwrap()),
        None,
        None,
        None,
    )
    .unwrap();

    // Create Bucket in REGION for BUCKET
    let bucket = Bucket::new_with_path_style("zenith-testbucket", region, credentials)?;

    loop {
        // List out contents of directory
        let results = bucket
            .list("walarchive/".to_string(), Some("".to_string()))
            .await?;
        let listing = results
            .iter()
            .flat_map(|b| b.contents.iter().map(|o| o.key.clone()))
            .collect();

        let n = offload_files(&bucket, &listing, &conf.workdir, conf).await?;
        info!("Offload {} files to S3", n);
        sleep(conf.ttl.unwrap()).await;
    }
}
