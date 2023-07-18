// Download extension files from the extension store
// and put them in the right place in the postgres directory
/*
The layout of the S3 bucket is as follows:

v14/ext_index.json
    -- this contains information necessary to create control files
v14/extensions/test_ext1.tar.gz
    -- this contains the library files and sql files necessary to create this extension
v14/extensions/custom_ext1.tar.gz

The difference between a private and public extensions is determined by who can
load the extension this is specified in ext_index.json

Speicially, ext_index.json has a list of public extensions, and a list of
extensions enabled for specific tenant-ids.
*/
use crate::compute::ComputeNode;
use anyhow::Context;
use anyhow::{self, Result};
use flate2::read::GzDecoder;
use remote_storage::*;
use serde_json::{self, Value};
use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::thread;
use tar::Archive;
use tokio::io::AsyncReadExt;
use tracing::info;

fn get_pg_config(argument: &str, pgbin: &str) -> String {
    // gives the result of `pg_config [argument]`
    // where argument is a flag like `--version` or `--sharedir`
    let pgconfig = pgbin
        .strip_suffix("postgres")
        .expect("bad pgbin")
        .to_owned()
        + "/pg_config";
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
    custom_ext_prefixes: &[String],
) -> Result<HashSet<String>> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let index_path = pg_version.to_owned() + "/ext_index.json";
    let index_path = RemotePath::new(Path::new(&index_path)).context("error forming path")?;
    info!("download ext_index.json: {:?}", &index_path);

    // TODO: potential optimization: cache ext_index.json
    let mut download = remote_storage.download(&index_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    let ext_index_str = match str::from_utf8(&write_data_buffer) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };

    let ext_index_full: Value = serde_json::from_str(ext_index_str)?;
    let ext_index_full = ext_index_full.as_object().context("error parsing json")?;
    let control_data = ext_index_full["control_data"]
        .as_object()
        .context("json parse error")?;
    let enabled_extensions = ext_index_full["enabled_extensions"]
        .as_object()
        .context("json parse error")?;
    info!("{:?}", control_data.clone());
    info!("{:?}", enabled_extensions.clone());

    let mut prefixes = vec!["public".to_string()];
    prefixes.extend(custom_ext_prefixes.to_owned());
    info!("{:?}", &prefixes);
    let mut all_extensions = HashSet::new();
    for prefix in prefixes {
        let prefix_extensions = match enabled_extensions.get(&prefix) {
            Some(Value::Array(ext_name)) => ext_name,
            _ => {
                info!("prefix {} has no extensions", prefix);
                continue;
            }
        };
        info!("{:?}", prefix_extensions);
        for ext_name in prefix_extensions {
            all_extensions.insert(ext_name.as_str().context("json parse error")?.to_string());
        }
    }

    for prefix in &all_extensions {
        let control_contents = control_data[prefix].as_str().context("json parse error")?;
        let control_path = local_sharedir.join(prefix.to_owned() + ".control");

        info!("WRITING FILE {:?}{:?}", control_path, control_contents);
        std::fs::write(control_path, control_contents)?;
    }

    Ok(all_extensions.into_iter().collect())
}

// download all sqlfiles (and possibly data files) for a given extension name
pub async fn download_extension(
    ext_name: &str,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    pg_version: &str,
) -> Result<()> {
    // TODO: potential optimization: only download the extension if it doesn't exist
    // problem: how would we tell if it exists?
    let ext_name = ext_name.replace(".so", "");
    let ext_name_targz = ext_name.to_owned() + ".tar.gz";
    if Path::new(&ext_name_targz).exists() {
        info!("extension {:?} already exists", ext_name_targz);
        return Ok(());
    }
    let ext_path = RemotePath::new(
        &Path::new(pg_version)
            .join("extensions")
            .join(ext_name_targz.clone()),
    )?;
    info!(
        "Start downloading extension {:?} from {:?}",
        ext_name, ext_path
    );
    let mut download = remote_storage.download(&ext_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    let unzip_dest = pgbin.strip_suffix("/bin/postgres").expect("bad pgbin");
    let tar = GzDecoder::new(write_data_buffer.as_slice());
    let mut archive = Archive::new(tar);
    archive.unpack(unzip_dest)?;
    info!("Download + unzip {:?} completed successfully", &ext_path);

    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let zip_sharedir = format!("{unzip_dest}/extensions/{ext_name}/share/extension");
    info!("mv {zip_sharedir:?}/* {local_sharedir:?}");
    for file in std::fs::read_dir(zip_sharedir)? {
        let old_file = file?.path();
        let new_file =
            Path::new(&local_sharedir).join(old_file.file_name().context("error parsing file")?);
        std::fs::rename(old_file, new_file)?;
    }
    let local_libdir = Path::new(&get_pg_config("--libdir", pgbin)).join("postgresql");
    let zip_libdir = format!("{unzip_dest}/extensions/{ext_name}/lib");
    info!("mv {zip_libdir:?}/* {local_libdir:?}");
    for file in std::fs::read_dir(zip_libdir)? {
        let old_file = file?.path();
        let new_file =
            Path::new(&local_libdir).join(old_file.file_name().context("error parsing file")?);
        std::fs::rename(old_file, new_file)?;
    }
    Ok(())
}

// This function initializes the necessary structs to use remote storage (should be fairly cheap)
pub fn init_remote_storage(
    remote_ext_config: &str,
    default_prefix: &str,
) -> anyhow::Result<GenericRemoteStorage> {
    let remote_ext_config: serde_json::Value = serde_json::from_str(remote_ext_config)?;

    let remote_ext_bucket = remote_ext_config["bucket"]
        .as_str()
        .context("config parse error")?;
    let remote_ext_region = remote_ext_config["region"]
        .as_str()
        .context("config parse error")?;
    let remote_ext_endpoint = remote_ext_config["endpoint"].as_str();
    let remote_ext_prefix = remote_ext_config["prefix"]
        .as_str()
        .unwrap_or(default_prefix)
        .to_string();

    // control plane passes the aws creds via CLI ARGS to compute_ctl
    let aws_key = remote_ext_config["key"].as_str();
    let aws_id = remote_ext_config["id"].as_str();
    if aws_key.is_some() && aws_id.is_some() {
        std::env::set_var("AWS_SECRET_ACCESS_KEY", aws_key.expect("is_some"));
        std::env::set_var("AWS_ACCESS_KEY_ID", aws_id.expect("is_some"));
    }

    // If needed, it is easy to allow modification of other parameters
    // however, default values should be fine for now
    let config = S3Config {
        bucket_name: remote_ext_bucket.to_string(),
        bucket_region: remote_ext_region.to_string(),
        prefix_in_bucket: Some(remote_ext_prefix),
        endpoint: remote_ext_endpoint.map(|x| x.to_string()),
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
