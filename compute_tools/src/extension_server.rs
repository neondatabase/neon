use anyhow::{self};
use remote_storage::*;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use tokio::io::AsyncReadExt;
use tracing::info;

pub async fn download_file(
    filename: &str,
    remote_ext_bucket: String,
    remote_ext_region: String,
) -> anyhow::Result<()> {
    // probably should be using the pgbin argv somehow to compute sharedir...
    let sharedir = get_pg_config("--sharedir");
    fs::write("alek/sharedir.txt", sharedir)?;

    println!("requested file {}", filename);

    // TODO: download the extensions!
    let s3_config = create_s3_config(remote_ext_bucket, remote_ext_region);
    download_extension(&s3_config, ExtensionType::Shared).await?;

    // This is filler code
    // let from_prefix = "/tmp/from_prefix";
    // let to_prefix = "/tmp/to_prefix";

    // let filepath = Path::new(from_prefix).join(filename);
    // let copy_to_filepath = Path::new(to_prefix).join(filename);
    // fs::copy(filepath, copy_to_filepath)?;

    Ok(())
}

// FIXME: this function panics if it runs into any issues
fn get_pg_config(argument: &str) -> String {
    let config_output = std::process::Command::new("pg_config")
        .arg(argument)
        .output()
        .expect("pg_config should be installed");
    assert!(config_output.status.success());
    let stdout = std::str::from_utf8(&config_output.stdout).unwrap();
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

/*
separate stroage and compute
storage: pageserver stores pages, accepts WAL. communicates with S3.
compute: postgres, runs in kubernetes/ VM, started by compute_ctl. rust service. accepts some spec.

pass config to compute_ctl
 */
pub async fn download_extension(
    config: &RemoteStorageConfig,
    ext_type: ExtensionType,
) -> anyhow::Result<()> {
    let sharedir = get_pg_config("--sharedir");
    let sharedir = format!("{}/extension", sharedir);
    let libdir = get_pg_config("--libdir");
    let remote_storage = GenericRemoteStorage::from_config(config)?;

    std::fs::write("alek/proof", "proof")?;

    // // this is just for testing doing a testing thing
    // let folder = RemotePath::new(Path::new("public_extensions"))?;
    // let from_paths = remote_storage.list_files(Some(&folder)).await?;
    // let some_path = from_paths[0]
    //     .object_name()
    //     .expect("had a problem with somepath in extension server");
    // fs::write("alek/SOMEPATH", some_path)?;

    // // this is the real thing
    // match ext_type {
    //     ExtensionType::Shared => {
    //         // 1. Download control files from s3-bucket/public/*.control to SHAREDIR/extension
    //         // We can do this step even before we have spec,
    //         // because public extensions are common for all projects.
    //         let folder = RemotePath::new(Path::new("public_extensions"))?;
    //         let from_paths = remote_storage.list_files(Some(&folder)).await?;
    //         for remote_from_path in from_paths {
    //             if remote_from_path.extension() == Some("control") {
    //                 // NOTE: if you run this, it will actually write stuff to your postgress directory
    //                 // only run if you are ok with that. TODO: delete this comment
    //                 download_helper(&remote_storage, &remote_from_path, &sharedir).await?;
    //             }
    //         }
    //     }
    //     ExtensionType::Tenant(tenant_id) => {
    //         // 2. After we have spec, before project start
    //         // Download control files from s3-bucket/[tenant-id]/*.control to SHAREDIR/extension
    //         let folder = RemotePath::new(Path::new(&format!("{tenant_id}")))?;
    //         let from_paths = remote_storage.list_files(Some(&folder)).await?;
    //         for remote_from_path in from_paths {
    //             if remote_from_path.extension() == Some("control") {
    //                 download_helper(&remote_storage, &remote_from_path, &sharedir).await?;
    //             }
    //         }
    //     }
    //     ExtensionType::Library(library_name) => {
    //         // 3. After we have spec, before postgres start
    //         // Download preload_shared_libraries from s3-bucket/public/[library-name].control into LIBDIR/
    //         let from_path = format!("neon-dev-extensions/public/{library_name}.control");
    //         let remote_from_path = RemotePath::new(Path::new(&from_path))?;
    //         download_helper(&remote_storage, &remote_from_path, &libdir).await?;
    //     }
    // }
    Ok(())
}

// TODO: add support for more of these parameters being configurable?
pub fn create_s3_config(
    remote_ext_bucket: String,
    remote_ext_region: String,
) -> RemoteStorageConfig {
    let config = S3Config {
        bucket_name: remote_ext_bucket,
        bucket_region: remote_ext_region,
        prefix_in_bucket: None,
        endpoint: None,
        concurrency_limit: NonZeroUsize::new(100).expect("100 != 0"),
        max_keys_per_list_response: None,
    };
    RemoteStorageConfig {
        max_concurrent_syncs: NonZeroUsize::new(100).expect("100 != 0"),
        max_sync_errors: NonZeroU32::new(100).expect("100 != 0"),
        storage: RemoteStorageKind::AwsS3(config),
    }
}
