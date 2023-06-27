use anyhow::{self, bail, Result};
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

// download extension control files
//
// if private_ext_prefixes is provided - search also in private extension paths
//
pub async fn get_available_extensions(
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    private_ext_prefixes: &Vec<String>,
) -> anyhow::Result<()> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let pg_version = get_pg_version(pgbin);

    // 1. Download public extension control files
    let remote_sharedir =
        RemotePath::new(&Path::new(&pg_version).join("share/postgresql/extension"))?;
    let from_paths: Vec<RemotePath> = remote_storage.list_files(Some(&remote_sharedir)).await?;

    info!(
        "get_available_extensions remote_sharedir: {:?}, local_sharedir: {:?}, \nall_paths: {:?}",
        remote_sharedir, local_sharedir, &from_paths
    );

    for remote_from_path in &from_paths {
        if remote_from_path.extension() == Some("control") {
            download_helper(remote_storage, remote_from_path, &local_sharedir).await?;
        }
    }

    // 2. Download private extension control files
    for private_prefix in private_ext_prefixes {
        let remote_sharedir_private = RemotePath::new(
            &Path::new(&pg_version)
                .join(private_prefix)
                .join("share/postgresql/extension"),
        )?;
        let from_paths_private: Vec<RemotePath> = remote_storage
            .list_files(Some(&remote_sharedir_private))
            .await?;

        info!(
            "get_available_extensions remote_sharedir_private: {:?}, local_sharedir: {:?}",
            remote_sharedir_private, local_sharedir
        );

        // download all found private control files
        for remote_from_path in &from_paths_private {
            if remote_from_path.extension() == Some("control") {
                download_helper(remote_storage, remote_from_path, &local_sharedir).await?;
            }
        }
    }

    Ok(())
}

// Download requested shared_preload_libraries
//
// Note that tenant_id is not optional here, because we only download libraries
// after we know the tenant spec and the tenant_id.
//
// return list of all library files to use it in the future searches
pub async fn get_available_libraries(
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    private_ext_prefixes: &Vec<String>,
    preload_libraries: &Vec<String>,
) -> anyhow::Result<()> {
    // Return early if there are no libraries to download
    if preload_libraries.is_empty() {
        return Ok(());
    }

    let local_libdir: PathBuf = Path::new(&get_pg_config("--pkglibdir", pgbin)).into();
    let pg_version = get_pg_version(pgbin);
    let remote_libdir = RemotePath::new(&Path::new(&pg_version).join("lib/")).unwrap();

    // 1. Download public libraries

    let available_libraries = remote_storage.list_files(Some(&remote_libdir)).await?;
    info!("list of library files {:?}", &available_libraries);

    // download all requested libraries
    // add file extension if it isn't in the filename
    for lib_name in preload_libraries {
        let lib_name_with_ext = if !lib_name.ends_with(".so") {
            lib_name.to_owned() + ".so"
        } else {
            lib_name.to_string()
        };

        info!("looking for library {:?}", &lib_name_with_ext);

        for lib in available_libraries.iter() {
            info!("object_name {}", lib.object_name().unwrap());
        }

        let lib_path = available_libraries
            .iter()
            .find(|lib: &&RemotePath| lib.object_name().unwrap() == lib_name_with_ext);

        match lib_path {
            // TODO don't panic here,
            // remember error and return it only if library is not found in any prefix
            None => bail!("Shared library file {lib_name} is not found in the extension store"),
            Some(lib_path) => {
                download_helper(remote_storage, lib_path, &local_libdir).await?;
                info!("downloaded library {:?}", &lib_path);
            }
        }
    }

    // 2. Download private libraries
    for private_prefix in private_ext_prefixes {
        let remote_libdir_private =
            RemotePath::new(&Path::new(&pg_version).join(private_prefix).join("lib/")).unwrap();
        let available_libraries_private = remote_storage
            .list_files(Some(&remote_libdir_private))
            .await?;
        info!("list of library files {:?}", &available_libraries_private);

        // download all requested libraries
        // add file extension if it isn't in the filename
        //
        // TODO refactor this code to avoid duplication
        for lib_name in preload_libraries {
            let lib_name_with_ext = if !lib_name.ends_with(".so") {
                lib_name.to_owned() + ".so"
            } else {
                lib_name.to_string()
            };

            info!("looking for library {:?}", &lib_name_with_ext);

            for lib in available_libraries_private.iter() {
                info!("object_name {}", lib.object_name().unwrap());
            }

            let lib_path = available_libraries_private
                .iter()
                .find(|lib: &&RemotePath| lib.object_name().unwrap() == lib_name_with_ext);

            match lib_path {
                None => bail!("Shared library file {lib_name} is not found in the extension store"),
                Some(lib_path) => {
                    download_helper(remote_storage, lib_path, &local_libdir).await?;
                    info!("downloaded library {:?}", &lib_path);
                }
            }
        }
    }

    Ok(())
}

// download all sql files for a given extension name
//
pub async fn download_extension_sql_files(
    ext_name: &str,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
) -> Result<()> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");

    let pg_version = get_pg_version(pgbin);
    let remote_sharedir: RemotePath =
        RemotePath::new(&Path::new(&pg_version).join("share/postgresql/extension"))?;
    let available_extensions = remote_storage.list_files(Some(&remote_sharedir)).await?;

    info!(
        "list of available_extension files {:?}",
        &available_extensions
    );

    // check if extension files exist
    let files_to_download: Vec<&RemotePath> = available_extensions
        .iter()
        .filter(|ext| {
            ext.extension() == Some("sql") && ext.object_name().unwrap().starts_with(ext_name)
        })
        .collect();

    if files_to_download.is_empty() {
        bail!("Files for extension {ext_name} are not found in the extension store");
    }

    for remote_from_path in files_to_download {
        download_helper(remote_storage, remote_from_path, &local_sharedir).await?;
    }

    Ok(())
}

// download shared library file
pub async fn download_library_file(
    lib_name: &str,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
) -> Result<()> {
    let local_libdir: PathBuf = Path::new(&get_pg_config("--pkglibdir", pgbin)).into();

    let pg_version = get_pg_version(pgbin);
    let remote_libdir = RemotePath::new(&Path::new(&pg_version).join("lib/")).unwrap();

    let available_libraries = remote_storage.list_files(Some(&remote_libdir)).await?;

    info!("list of library files {:?}", &available_libraries);

    // check if the library file exists
    let lib = available_libraries
        .iter()
        .find(|lib: &&RemotePath| lib.object_name().unwrap() == lib_name);

    match lib {
        None => bail!("Shared library file {lib_name} is not found in the extension store"),
        Some(lib) => {
            download_helper(remote_storage, lib, &local_libdir).await?;
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
