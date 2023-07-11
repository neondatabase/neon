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
use anyhow::{self, bail, Result};
use remote_storage::*;
use serde_json::{self, Value};
use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::thread;
use tokio::io::AsyncReadExt;
use tracing::info;

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
) -> Result<HashSet<String>> {
    // TODO: in this function change expect's to pass the error instead of panic-ing

    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");

    let index_path = pg_version.to_owned() + "/ext_index.json";
    let index_path = RemotePath::new(Path::new(&index_path)).expect("error forming path");
    info!("download extension index json: {:?}", &index_path);
    let all_files = remote_storage.list_files(None).await?;

    dbg!(all_files);

    // TODO: if index_path already exists, don't re-download it, just read it.
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

    dbg!(ext_index_str);

    let ext_index_full = match serde_json::from_str(&ext_index_str) {
        Ok(Value::Object(map)) => map,
        _ => bail!("error parsing json"),
    };
    let control_data = ext_index_full["control_data"]
        .as_object()
        .expect("json parse error");
    let enabled_extensions = ext_index_full["enabled_extensions"]
        .as_object()
        .expect("json parse error");

    dbg!(ext_index_full.clone());
    dbg!(control_data.clone());
    dbg!(enabled_extensions.clone());

    let mut prefixes = vec!["public".to_string()];
    prefixes.extend(custom_ext_prefixes.clone());
    dbg!(prefixes.clone());
    let mut all_extensions = HashSet::new();
    for prefix in prefixes {
        let prefix_extensions = match enabled_extensions.get(&prefix) {
            Some(Value::Array(ext_name)) => ext_name,
            _ => {
                info!("prefix {} has no extensions", prefix);
                continue;
            }
        };
        dbg!(prefix_extensions);
        for ext_name in prefix_extensions {
            all_extensions.insert(ext_name.as_str().expect("json parse error").to_string());
        }
    }

    // TODO: this is probably I/O bound, could benefit from parallelizing
    for prefix in &all_extensions {
        let control_contents = control_data[prefix].as_str().expect("json parse error");
        let control_path = local_sharedir.join(prefix.to_owned() + ".control");

        info!("WRITING FILE {:?}{:?}", control_path, control_contents);
        std::fs::write(control_path, &control_contents)?;
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
    let ext_path = RemotePath::new(
        &Path::new(pg_version)
            .join("extensions")
            .join(ext_name.to_owned() + ".tar.gz"),
    )?;
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let local_libdir = Path::new(&get_pg_config("--libdir", pgbin)).to_owned();
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
