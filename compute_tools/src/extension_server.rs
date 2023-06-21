use anyhow::{self};
use remote_storage::*;
use serde_json::{self, Value};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use tokio::io::AsyncReadExt;
use tracing::info;

// TODO: get rid of this function by making s3_config part of ComputeNode
pub async fn download_file(filename: &str, remote_ext_config: &str) -> anyhow::Result<()> {
    let s3_config = create_s3_config(remote_ext_config)?;
    download_extension(&s3_config, ExtensionType::Shared).await?;
    Ok(())
}

fn get_pg_config(argument: &str) -> String {
    let config_output = std::process::Command::new("pg_config")
        .arg(argument)
        .output()
        .expect("pg_config must be installed");
    assert!(config_output.status.success());
    let stdout = std::str::from_utf8(&config_output.stdout).expect("error obtaining pg_config");
    stdout.trim().to_string()
}

async fn download_helper(
    remote_storage: &GenericRemoteStorage,
    remote_from_path: &RemotePath,
    to_path: &str,
) -> anyhow::Result<()> {
    let file_name = remote_from_path.object_name().expect("it must exist");
    info!("Downloading {:?}", file_name);
    info!(
        "To location {:?} (actually just downloading  it with it's remote name for now at least)",
        to_path
    );
    let mut download = remote_storage.download(&remote_from_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    let mut output_file = BufWriter::new(File::create(file_name)?);
    output_file.write_all(&mut write_data_buffer)?;
    Ok(())
}

pub enum ExtensionType {
    Shared,          // we just use the public folder here
    Tenant(String),  // String is tenant_id
    Library(String), // String is name of the extension
}

pub async fn download_extension(
    config: &RemoteStorageConfig,
    ext_type: ExtensionType,
) -> anyhow::Result<()> {
    let remote_storage = GenericRemoteStorage::from_config(config)?;

    let from_paths = remote_storage.list_files(None).await?;
    std::fs::write("ALEK_LIST_FILES.txt", format!("{:?}", from_paths))?;

    // TODO: probably should be using the pgbin argv somehow to compute sharedir...,
    // right now it is getting my global pg_config, which is wrong
    let sharedir = get_pg_config("--sharedir");
    let sharedir = format!("{}/extension", sharedir);
    let libdir = get_pg_config("--libdir");
    match ext_type {
        ExtensionType::Shared => {
            // 1. Download control files from s3-bucket/public/*.control to SHAREDIR/extension
            // We can do this step even before we have spec,
            // because public extensions are common for all projects.
            let folder = RemotePath::new(Path::new("public_extensions"))?;
            let from_paths = remote_storage.list_files(Some(&folder)).await?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    download_helper(&remote_storage, &remote_from_path, &sharedir).await?;
                }
            }
        }
        ExtensionType::Tenant(tenant_id) => {
            // 2. After we have spec, before project start
            // Download control files from s3-bucket/[tenant-id]/*.control to SHAREDIR/extension
            let folder = RemotePath::new(Path::new(&format!("{tenant_id}")))?;
            let from_paths = remote_storage.list_files(Some(&folder)).await?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    download_helper(&remote_storage, &remote_from_path, &sharedir).await?;
                }
            }
        }
        ExtensionType::Library(library_name) => {
            // 3. After we have spec, before postgres start
            // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/
            let from_path = format!("neon-dev-extensions/public/{library_name}.control");
            let remote_from_path = RemotePath::new(Path::new(&from_path))?;
            download_helper(&remote_storage, &remote_from_path, &libdir).await?;
        }
    }
    Ok(())
}

pub fn create_s3_config(remote_ext_config: &str) -> anyhow::Result<RemoteStorageConfig> {
    let remote_ext_config: serde_json::Value = serde_json::from_str(remote_ext_config)?;
    let remote_ext_bucket = match &remote_ext_config["bucket"] {
        Value::String(x) => x,
        _ => panic!("oops"),
    };
    let remote_ext_region = match &remote_ext_config["region"] {
        Value::String(x) => x,
        _ => panic!("oops"),
    };
    let remote_ext_endpoint = match &remote_ext_config["endpoint"] {
        Value::String(x) => Some(x.clone()),
        _ => None,
    };

    // load will not be large, so default parameters are fine
    let config = S3Config {
        bucket_name: remote_ext_bucket.clone(),
        bucket_region: remote_ext_region.clone(),
        prefix_in_bucket: None,
        endpoint: remote_ext_endpoint,
        concurrency_limit: NonZeroUsize::new(100).expect("100 != 0"),
        max_keys_per_list_response: None,
    };
    Ok(RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).expect("100 != 0"),
        max_sync_errors: NonZeroU32::new(100).expect("100 != 0"),
        storage: RemoteStorageKind::AwsS3(config),
    })
}
