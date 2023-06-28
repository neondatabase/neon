// Download extension files from the extension store
// and put them in the right place in the postgres directory
use anyhow::{self, bail, Result};
use remote_storage::*;
use serde_json::{self, Value};
use std::collections::HashMap;
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

async fn download_helper(
    remote_storage: &GenericRemoteStorage,
    remote_from_path: &RemotePath,
    remote_from_prefix: Option<&Path>,
    download_location: &Path,
) -> anyhow::Result<()> {
    // downloads file at remote_from_path to download_location/[file_name]

    // we cannot use remote_from_path.object_name() here
    // because extension files can be in subdirectories of the extension store.
    //
    // To handle this, we use remote_from_prefix to strip the prefix from the path
    // this gives us the relative path of the file in the extension store,
    // and we use this relative path to construct the local path.
    //
    let local_path = match remote_from_prefix {
        Some(prefix) => {
            let p = remote_from_path
                .get_path()
                .strip_prefix(prefix)
                .expect("bad prefix");

            download_location.join(p)
        }

        None => download_location.join(remote_from_path.object_name().expect("bad object")),
    };

    if local_path.exists() {
        info!("File {:?} already exists. Skipping download", &local_path);
        return Ok(());
    }

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
    //dbg!(str::from_utf8(&write_data_buffer)?);
    if remote_from_prefix.is_some() {
        if let Some(prefix) = local_path.parent() {
            info!(
                "Downloading file with prefix. create directory {:?}",
                prefix
            );
            std::fs::create_dir_all(prefix)?;
        }
    }

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
    pg_version: &str,
    private_ext_prefixes: &Vec<String>,
) -> anyhow::Result<()> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");

    let mut paths: Vec<RemotePath> = Vec::new();
    // public extensions
    paths.push(RemotePath::new(
        &Path::new(pg_version).join("share/postgresql/extension"),
    )?);
    // private extensions
    for private_prefix in private_ext_prefixes {
        paths.push(RemotePath::new(
            &Path::new(pg_version)
                .join(private_prefix)
                .join("share/postgresql/extension"),
        )?);
    }

    let all_available_files = list_files_in_prefixes(remote_storage, &paths).await?;

    info!(
        "list of available_extension files {:?}",
        &all_available_files
    );

    // download all control files
    for (obj_name, obj_path) in &all_available_files {
        if obj_name.ends_with("control") {
            download_helper(remote_storage, obj_path, None, &local_sharedir).await?;
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
    pg_version: &str,
    private_ext_prefixes: &Vec<String>,
    preload_libraries: &Vec<String>,
) -> anyhow::Result<HashMap<String, RemotePath>> {
    let local_libdir: PathBuf = Path::new(&get_pg_config("--pkglibdir", pgbin)).into();
    // Construct a hashmap of all available libraries
    // example (key, value) pair: test_lib0.so, v14/lib/test_lib0.so

    let mut paths: Vec<RemotePath> = Vec::new();
    // public libraries
    paths.push(RemotePath::new(&Path::new(&pg_version).join("lib/")).unwrap());
    // private libraries
    for private_prefix in private_ext_prefixes {
        paths.push(
            RemotePath::new(&Path::new(&pg_version).join(private_prefix).join("lib")).unwrap(),
        );
    }

    let all_available_libraries = list_files_in_prefixes(remote_storage, &paths).await?;

    info!("list of library files {:?}", &all_available_libraries);

    // download all requested libraries
    for lib_name in preload_libraries {
        // add file extension if it isn't in the filename
        let lib_name_with_ext = enforce_so_end(&lib_name);
        info!("looking for library {:?}", &lib_name_with_ext);

        match all_available_libraries.get(&*lib_name_with_ext) {
            Some(remote_path) => {
                download_helper(remote_storage, remote_path, None, &local_libdir).await?
            }
            None => {
                let file_path = local_libdir.join(&lib_name_with_ext);
                if file_path.exists() {
                    info!("File {:?} already exists. Skipping download", &file_path);
                } else {
                    bail!("Shared library file {lib_name} is not found in the extension store")
                }
            }
        }
    }

    Ok(all_available_libraries)
}

// download all sqlfiles (and possibly data files) for a given extension name
//
pub async fn download_extension_sql_files(
    ext_name: &str,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    pg_version: &str,
    private_ext_prefixes: &Vec<String>,
) -> Result<()> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");

    let mut paths: Vec<RemotePath> = Vec::new();
    // public extensions
    paths.push(RemotePath::new(
        &Path::new(&pg_version).join("share/postgresql/extension"),
    )?);
    // private extensions
    for private_prefix in private_ext_prefixes {
        paths.push(RemotePath::new(
            &Path::new(&pg_version)
                .join(private_prefix)
                .join("share/postgresql/extension"),
        )?);
    }

    let all_available_files: HashMap<String, RemotePath> =
        list_files_in_prefixes(remote_storage, &paths).await?;

    info!(
        "list of available_extension files {:?}",
        &all_available_files
    );

    // check if extension files exist
    let mut files_to_download: Vec<(&RemotePath, Option<&Path>)> = Vec::new();

    for (obj_name, obj_path) in &all_available_files {
        // ignore control files
        if !obj_name.ends_with("control") {
            // We can't use just ext.object_name() here
            // because extension files can be in subdirectories of the extension store.
            // examples of layout:
            //
            // share/postgresql/extension/extension_name--1.0.sql
            //
            // or
            //
            // share/postgresql/extension/extension_name/extension_name--1.0.sql
            // share/postgresql/extension/extension_name/extra_data.csv
            //
            for prefix in paths.iter() {
                if let Ok(full_object_name) = obj_path.get_path().strip_prefix(prefix.get_path()) {
                    if full_object_name.to_str().unwrap().starts_with(ext_name) {
                        files_to_download.push((obj_path, Some(prefix.get_path())));
                    }
                }
            }
        }
    }

    if files_to_download.is_empty() {
        bail!("Files for extension {ext_name} are not found in the extension store");
    }

    for (remote_from_path, remote_from_prefix) in files_to_download {
        download_helper(
            remote_storage,
            remote_from_path,
            remote_from_prefix,
            &local_sharedir,
        )
        .await?;
    }

    Ok(())
}

// appends an .so suffix to libname if it does not already have one
fn enforce_so_end(libname: &str) -> String {
    if !libname.ends_with(".so") {
        format!("{}.so", libname)
    } else {
        libname.to_string()
    }
}

// download shared library file
pub async fn download_library_file(
    lib_name: &str,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    all_available_libraries: &HashMap<String, RemotePath>,
) -> Result<()> {
    let local_libdir: PathBuf = Path::new(&get_pg_config("--pkglibdir", pgbin)).into();
    let lib_name_with_ext = enforce_so_end(lib_name);
    info!("looking for library {:?}", &lib_name_with_ext);
    match all_available_libraries.get(&*lib_name_with_ext) {
        Some(remote_path) => {
            download_helper(remote_storage, remote_path, None, &local_libdir).await?
        }
        None => bail!("Shared library file {lib_name} is not found in the extension store"),
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

// helper to collect all files in the given prefixes
// returns hashmap of (file_name, file_remote_path)
async fn list_files_in_prefixes(
    remote_storage: &GenericRemoteStorage,
    paths: &Vec<RemotePath>,
) -> Result<HashMap<String, RemotePath>> {
    let mut res = HashMap::new();

    for path in paths {
        for file in remote_storage.list_files(Some(&path)).await? {
            res.insert(
                file.object_name().expect("bad object").to_owned(),
                file.to_owned(),
            );
        }
    }

    Ok(res)
}
