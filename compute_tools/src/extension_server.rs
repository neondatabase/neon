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

fn get_pg_config(argument: &str, pgbin: &str) -> (String, String) {
    let mut pgconfig = String::from(pgbin.strip_suffix("postgres").expect("pg_config error"));
    pgconfig.push_str("pg_config");

    let config_output = std::process::Command::new(pgconfig)
        .arg(argument)
        .output()
        .expect("pg_config must be installed");
    assert!(config_output.status.success());
    let local_path = std::str::from_utf8(&config_output.stdout)
        .expect("error obtaining pg_config")
        .trim()
        .to_string();

    let mut rm_prefix: String = std::env::current_dir()
        .expect("pg_config error")
        .to_str()
        .expect("pg_config error")
        .into();
    rm_prefix.push_str("/pg_install/");
    let remote_path = local_path
        .strip_prefix(&rm_prefix)
        .expect("pg_config error")
        .trim()
        .to_string();

    (local_path, remote_path)
}

async fn download_helper(
    remote_storage: &GenericRemoteStorage,
    remote_from_path: &RemotePath,
    download_to_dir: &str,
) -> anyhow::Result<()> {
    std::fs::write("ALEK_DOWNLOAD.txt", format!("{:?}", download_to_dir))?;
    let file_name = remote_from_path.object_name().expect("it must exist");
    info!("Downloading {:?}", file_name);
    info!("To location {:?}", download_to_dir);
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
    remote_storage: &GenericRemoteStorage,
    ext_type: ExtensionType,
    pgbin: &str,
) -> anyhow::Result<()> {
    let from_paths = remote_storage.list_files(None).await?;
    std::fs::write("ALEK_LIST_FILES.txt", format!("{:?}", from_paths))?;

    let (mut local_sharedir, mut remote_sharedir) = get_pg_config("--sharedir", pgbin);
    local_sharedir.push_str("/extension");
    remote_sharedir.push_str("/extension");
    std::fs::write("ALEK_SHAREDIR.txt", format!("{:?}", remote_sharedir))?;
    let (local_libdir, _) = get_pg_config("--libdir", pgbin);
    match ext_type {
        ExtensionType::Shared => {
            // 1. Download control files from s3-bucket/public/*.control to SHAREDIR/extension
            // We can do this step even before we have spec,
            // because public extensions are common for all projects.
            let folder = RemotePath::new(Path::new(&remote_sharedir))?;
            let from_paths = remote_storage.list_files(Some(&folder)).await?;
            std::fs::write(
                "ALEK_QUEUE_DOWNLOAD.txt",
                format!("{:?}", from_paths.clone()),
            )?;
            for remote_from_path in from_paths {
                if remote_from_path.extension() == Some("control") {
                    download_helper(&remote_storage, &remote_from_path, &local_sharedir).await?;
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
                    download_helper(&remote_storage, &remote_from_path, &local_sharedir).await?;
                }
            }
        }
        ExtensionType::Library(library_name) => {
            // 3. After we have spec, before postgres start
            // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/
            let from_path = format!("neon-dev-extensions/public/{library_name}.control");
            let remote_from_path = RemotePath::new(Path::new(&from_path))?;
            download_helper(&remote_storage, &remote_from_path, &local_libdir).await?;
        }
    }
    Ok(())
}

pub fn init_remote_storage(remote_ext_config: &str) -> anyhow::Result<GenericRemoteStorage> {
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
    let config = RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).expect("100 != 0"),
        max_sync_errors: NonZeroU32::new(100).expect("100 != 0"),
        storage: RemoteStorageKind::AwsS3(config),
    };
    Ok(GenericRemoteStorage::from_config(&config)?)
}
