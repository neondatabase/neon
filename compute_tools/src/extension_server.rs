use anyhow::{self, bail};
use remote_storage::*;
use serde_json::{self, Value};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str;
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

fn get_pg_version(pgbin: &str) -> String {
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

async fn download_helper(
    remote_storage: &GenericRemoteStorage,
    remote_from_path: &RemotePath,
    download_location: &Path,
) -> anyhow::Result<()> {
    // downloads file at remote_from_path to download_location/[file_name]
    let local_path = download_location.join(remote_from_path.object_name().expect("bad object"));
    info!(
        "Downloading {:?} to location {:?}",
        &remote_from_path, &local_path
    );
    let mut download = remote_storage.download(remote_from_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    dbg!(str::from_utf8(&write_data_buffer)?);
    let mut output_file = BufWriter::new(File::create(local_path)?);
    output_file.write_all(&write_data_buffer)?;
    Ok(())
}

pub enum ExtensionType {
    Shared,          // we just use the public folder here
    Tenant(String),  // String is tenant_id
    Library(String), // String is name of the extension
}

pub async fn download_extension(
    remote_storage: &Option<GenericRemoteStorage>,
    ext_type: ExtensionType,
    pgbin: &str,
) -> anyhow::Result<()> {
    let remote_storage = match remote_storage {
        Some(remote_storage) => remote_storage,
        None => return Ok(()),
    };
    let pg_version = get_pg_version(pgbin);
    match ext_type {
        ExtensionType::Shared => {
            // 1. Download control files from s3-bucket/public/*.control to SHAREDIR/extension
            // We can do this step even before we have spec,
            // because public extensions are common for all projects.
            let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
            let remote_sharedir = Path::new(&pg_version).join("share/postgresql/extension");
            let remote_sharedir = RemotePath::new(Path::new(&remote_sharedir))?;
            let from_paths = remote_storage.list_files(Some(&remote_sharedir)).await?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    download_helper(remote_storage, &remote_from_path, &local_sharedir).await?;
                }
            }
        }
        ExtensionType::Tenant(tenant_id) => {
            // 2. After we have spec, before project start
            // Download control files from s3-bucket/[tenant-id]/*.control to SHAREDIR/extension

            let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
            let remote_path = RemotePath::new(Path::new(&tenant_id.to_string()))?;
            let from_paths = remote_storage.list_files(Some(&remote_path)).await?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    download_helper(remote_storage, &remote_from_path, &local_sharedir).await?;
                }
            }
        }
        ExtensionType::Library(library_name) => {
            // 3. After we have spec, before postgres start
            // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/

            let local_libdir: PathBuf = Path::new(&get_pg_config("--libdir", pgbin)).into();
            let remote_path = format!("{library_name}.control");
            let remote_from_path = RemotePath::new(Path::new(&remote_path))?;
            download_helper(remote_storage, &remote_from_path, &local_libdir).await?;
        }
    }
    Ok(())
}

pub fn init_remote_storage(remote_ext_config: &str) -> anyhow::Result<GenericRemoteStorage> {
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
        _ => None,
    };

    // load will not be large, so default parameters are fine
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
