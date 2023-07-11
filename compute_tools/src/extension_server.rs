// Download extension files from the extension store
// and put them in the right place in the postgres directory
use crate::compute::ComputeNode;
use anyhow::{self, bail, Result};
use remote_storage::*;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::thread;
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

fn get_pg_config(argument: &str, pgbin: &str) -> String {
    // gives the result of `pg_config [argument]`
    // where argument is a flag like `--version` or `--sharedir`
    let pgconfig = pgbin.replace("postgres", "pg_config");
    let config_output = std::process::Command::new(pgconfig)
        .arg(argument)
        .output()
        .expect("pg_config error");
    std::str::from_utf8(&config_output.stdout)
        .expect("pg_config error")
        .trim()
        .to_string()
}

pub fn get_pg_version(pgbin: &str) -> String {
    // pg_config --version returns a (platform specific) human readable string
    // such as "PostgreSQL 15.4". We parse this to v14/v15
    let human_version = get_pg_config("--version", pgbin);
    if human_version.contains("15") {
        return "v15".to_string();
    } else if human_version.contains("14") {
        return "v14".to_string();
    }
    panic!("Unsuported postgres version {human_version}");
}

// download extension control files
// if custom_ext_prefixes is provided - search also in custom extension paths
pub async fn get_available_extensions(
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    pg_version: &str,
    custom_ext_prefixes: &Vec<String>,
) -> Result<HashMap<String, RemotePath>> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");

    let index_path = RemotePath::new(Path::new(&format!("{:?}/ext_index.json", pg_version)))
        .expect("error forming path");
    let mut download = remote_storage.download(&index_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    let ext_index_str =
        serde_json::to_string(&write_data_buffer).expect("Failed to convert to JSON");
    let ext_index_full = match serde_json::from_str(&ext_index_str) {
        Ok(Value::Object(map)) => map,
        _ => bail!("error parsing json"),
    };

    let mut prefixes = vec!["public".to_string()];
    prefixes.extend(custom_ext_prefixes.clone());
    let mut ext_index_limited = HashMap::new();
    for prefix in prefixes {
        let ext_details_str = ext_index_full.get(&prefix);
        if let Some(ext_details_str) = ext_details_str {
            let ext_details =
                serde_json::to_string(ext_details_str).expect("Failed to convert to JSON");
            let ext_details = match serde_json::from_str(&ext_details) {
                Ok(Value::Object(map)) => map,
                _ => bail!("error parsing json"),
            };
            let control_contents = match ext_details.get("control").expect("broken json file") {
                Value::String(s) => s,
                _ => bail!("broken json file"),
            };
            let path = RemotePath::new(Path::new(&format!(
                "{:?}/{:?}",
                pg_version,
                ext_details.get("path")
            )))
            .expect("error forming path");

            let control_path = format!("{:?}/{:?}.control", &local_sharedir, &prefix);
            std::fs::write(control_path, &control_contents)?;

            ext_index_limited.insert(prefix, path);
        } else {
            warn!("BAD PREFIX {:?}", prefix);
        }
    }
    Ok(ext_index_limited)
}

// download all sqlfiles (and possibly data files) for a given extension name
//
pub async fn download_extension(
    ext_name: &str,
    ext_path: &RemotePath,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
) -> Result<()> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let local_libdir = Path::new(&get_pg_config("--libdir", pgbin)).to_owned();
    info!("Start downloading extension {:?}", ext_name);
    let mut download = remote_storage.download(&ext_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    let zip_name = ext_path.object_name().expect("invalid extension path");
    let mut output_file = BufWriter::new(File::create(zip_name)?);
    output_file.write_all(&write_data_buffer)?;
    info!("Download {:?} completed successfully", &ext_path);
    info!("Unzipping extension {:?}", zip_name);

    // TODO unzip and place files in appropriate locations
    info!("unzip {zip_name:?}");
    info!("place extension files in {local_sharedir:?}");
    info!("place library files in {local_libdir:?}");

    Ok(())
}

// This function initializes the necessary structs to use remmote storage (should be fairly cheap)
pub fn init_remote_storage(
    remote_ext_config: &str,
    default_prefix: &str,
) -> anyhow::Result<GenericRemoteStorage> {
    let remote_ext_config: serde_json::Value = serde_json::from_str(remote_ext_config)?;

    let remote_ext_bucket = match &remote_ext_config["bucket"] {
        Value::String(x) => x,
        _ => bail!("remote_ext_config missing bucket"),
    };
    let remote_ext_region = match &remote_ext_config["region"] {
        Value::String(x) => x,
        _ => bail!("remote_ext_config missing region"),
    };
    let remote_ext_endpoint = match &remote_ext_config["endpoint"] {
        Value::String(x) => Some(x.clone()),
        _ => None,
    };
    let remote_ext_prefix = match &remote_ext_config["prefix"] {
        Value::String(x) => Some(x.clone()),
        // if prefix is not provided, use default, which is the build_tag
        _ => Some(default_prefix.to_string()),
    };

    // TODO: is this a valid assumption? some extensions are quite large
    // load should not be large, so default parameters are fine
    let config = S3Config {
        bucket_name: remote_ext_bucket.to_string(),
        bucket_region: remote_ext_region.to_string(),
        prefix_in_bucket: remote_ext_prefix,
        endpoint: remote_ext_endpoint,
        concurrency_limit: NonZeroUsize::new(100).expect("100 != 0"),
        max_keys_per_list_response: None,
    };
    let config = RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).expect("100 != 0"),
        max_sync_errors: NonZeroU32::new(100).expect("100 != 0"),
        storage: RemoteStorageKind::AwsS3(config),
    };
    GenericRemoteStorage::from_config(&config)
}

pub fn launch_download_extensions(
    compute: &Arc<ComputeNode>,
) -> Result<thread::JoinHandle<()>, std::io::Error> {
    let compute = Arc::clone(compute);
    thread::Builder::new()
        .name("download-extensions".into())
        .spawn(move || {
            info!("start download_extension_files");
            let compute_state = compute.state.lock().expect("error unlocking compute.state");
            compute
                .prepare_external_extensions(&compute_state)
                .expect("error preparing extensions");
            info!("download_extension_files done, exiting thread");
        })
}
