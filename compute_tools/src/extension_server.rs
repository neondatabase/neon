// Download extension files from the extension store
// and put them in the right place in the postgres directory
use anyhow::{self, bail, Context, Result};
use futures::future::join_all;
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

// remote!
const SHARE_EXT_PATH: &str = "share/extension";

fn pass_any_error(results: Vec<Result<()>>) -> Result<()> {
    for result in results {
        result?;
    }
    Ok(())
}

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
    remote_from_path: RemotePath,
    sub_directory: Option<&str>,
    download_location: &Path,
) -> anyhow::Result<()> {
    // downloads file at remote_from_path to
    // `download_location/[optional: subdirectory]/[remote_storage.object_name()]`
    // Note: the subdirectory commmand is needed when there is an extension that
    // depends on files in a subdirectory.
    // For example, v14/share/extension/some_ext.control
    // might depend on v14/share/extension/some_ext/some_ext--1.1.0.sql
    // and v14/share/extension/some_ext/xxx.csv
    // Note: it is the caller's responsibility to create the appropriate subdirectory

    let local_path = match sub_directory {
        Some(subdir) => download_location
            .join(subdir)
            .join(remote_from_path.object_name().expect("bad object")),
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
    let mut download = remote_storage.download(&remote_from_path).await?;
    let mut write_data_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut write_data_buffer)
        .await?;
    let mut output_file = BufWriter::new(File::create(local_path)?);
    output_file.write_all(&write_data_buffer)?;
    info!("Download {:?} completed successfully", &remote_from_path);
    Ok(())
}

// download extension control files
//
// if custom_ext_prefixes is provided - search also in custom extension paths
//
pub async fn get_available_extensions(
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    pg_version: &str,
    custom_ext_prefixes: &Vec<String>,
) -> anyhow::Result<HashMap<String, Vec<PathAndFlag>>> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");

    // public path, plus any private paths to download extensions from
    let mut paths: Vec<RemotePath> = Vec::new();
    paths.push(RemotePath::new(
        &Path::new(pg_version).join(SHARE_EXT_PATH),
    )?);
    for custom_prefix in custom_ext_prefixes {
        paths.push(RemotePath::new(
            &Path::new(pg_version)
                .join(custom_prefix)
                .join(SHARE_EXT_PATH),
        )?);
    }

    let (extension_files, control_files) =
        organized_extension_files(remote_storage, &paths).await?;

    let mut control_file_download_tasks = Vec::new();
    // download all control files
    for control_file in control_files {
        control_file_download_tasks.push(download_helper(
            remote_storage,
            control_file.clone(),
            None,
            &local_sharedir,
        ));
    }
    pass_any_error(join_all(control_file_download_tasks).await)?;
    Ok(extension_files)
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
    custom_ext_prefixes: &Vec<String>,
    preload_libraries: &Vec<String>,
) -> anyhow::Result<HashMap<String, Vec<RemotePath>>> {
    // Construct a hashmap of all available libraries
    // example (key, value) pair: test_lib0: [RemotePath(v14/lib/test_lib0.so), RemotePath(v14/lib/test_lib0.so.3)]
    let mut paths: Vec<RemotePath> = Vec::new();
    // public libraries
    paths.push(
        RemotePath::new(&Path::new(&pg_version).join("lib/"))
            .expect("The hard coded path here is valid"),
    );
    // custom libraries
    for custom_prefix in custom_ext_prefixes {
        paths.push(
            RemotePath::new(&Path::new(&pg_version).join(custom_prefix).join("lib"))
                .expect("The hard coded path here is valid"),
        );
    }
    let all_available_libraries = organized_library_files(remote_storage, &paths).await?;

    info!("list of library files {:?}", &all_available_libraries);
    // download all requested libraries
    let mut download_tasks = Vec::new();
    for lib_name in preload_libraries {
        download_tasks.push(download_library_file(
            lib_name,
            remote_storage,
            pgbin,
            &all_available_libraries,
        ));
    }
    pass_any_error(join_all(download_tasks).await)?;
    Ok(all_available_libraries)
}

// download all sqlfiles (and possibly data files) for a given extension name
//
pub async fn download_extension_files(
    ext_name: &str,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    all_available_files: &HashMap<String, Vec<PathAndFlag>>,
) -> Result<()> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let mut downloaded_something = false;
    let mut made_subdir = false;

    info!("EXTENSION {:?}", ext_name);
    info!("{:?}", all_available_files.get(ext_name));

    info!("start download");
    let mut download_tasks = Vec::new();
    if let Some(files) = all_available_files.get(ext_name) {
        info!("Downloading files for extension {:?}", &ext_name);
        for path_and_flag in files {
            let file = &path_and_flag.path;
            let subdir_flag = path_and_flag.subdir_flag;
            info!(
                "--- Downloading {:?} (for {:?} as subdir? = {:?})",
                &file, &ext_name, subdir_flag
            );
            let mut subdir = None;
            if subdir_flag {
                subdir = Some(ext_name);
                if !made_subdir {
                    made_subdir = true;
                    std::fs::create_dir_all(local_sharedir.join(ext_name))?;
                }
            }
            download_tasks.push(download_helper(
                remote_storage,
                file.clone(),
                subdir,
                &local_sharedir,
            ));
            downloaded_something = true;
        }
    }
    if !downloaded_something {
        bail!("Files for extension {ext_name} are not found in the extension store");
    }
    pass_any_error(join_all(download_tasks).await)?;
    info!("finish download");
    Ok(())
}

// appends an .so suffix to libname if it does not already have one
fn enforce_so_end(libname: &str) -> String {
    if !libname.contains(".so") {
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
    all_available_libraries: &HashMap<String, Vec<RemotePath>>,
) -> Result<()> {
    let lib_name = get_library_name(lib_name);
    let local_libdir: PathBuf = Path::new(&get_pg_config("--pkglibdir", pgbin)).into();
    info!("looking for library {:?}", &lib_name);
    match all_available_libraries.get(&*lib_name) {
        Some(remote_paths) => {
            let mut library_download_tasks = Vec::new();
            for remote_path in remote_paths {
                let file_path = local_libdir.join(remote_path.object_name().expect("bad object"));
                if file_path.exists() {
                    info!("File {:?} already exists. Skipping download", &file_path);
                } else {
                    library_download_tasks.push(download_helper(
                        remote_storage,
                        remote_path.clone(),
                        None,
                        &local_libdir,
                    ));
                }
            }
            pass_any_error(join_all(library_download_tasks).await)?;
        }
        None => {
            // minor TODO: this logic seems to be somewhat faulty for .so.3 type files?
            let lib_name_with_ext = enforce_so_end(&lib_name);
            let file_path = local_libdir.join(lib_name_with_ext);
            if file_path.exists() {
                info!("File {:?} already exists. Skipping download", &file_path);
            } else {
                bail!("Library file {lib_name} not found")
            }
        }
    }
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

fn get_library_name(path: &str) -> String {
    let path_suffix: Vec<&str> = path.split('/').collect();
    let path_suffix = path_suffix.last().expect("bad ext name").to_string();
    if let Some(index) = path_suffix.find(".so") {
        return path_suffix[..index].to_string();
    }
    path_suffix
}

// asyncrounously lists files in all necessary directories
// TODO: potential optimization: do a single list files on the entire bucket
// and then filter out the files we don't need
async fn list_all_files(
    remote_storage: &GenericRemoteStorage,
    paths: &Vec<RemotePath>,
) -> Result<Vec<RemotePath>> {
    let mut list_tasks = Vec::new();
    let mut all_files = Vec::new();
    for path in paths {
        list_tasks.push(remote_storage.list_files(Some(path)));
    }
    for list_result in join_all(list_tasks).await {
        all_files.extend(list_result?);
    }
    Ok(all_files)
}

// helper to collect all libraries, grouped by library name
// Returns a hashmap of (library name: [paths]})
// example entry: {libpgtypes: [libpgtypes.so.3, libpgtypes.so]}
async fn organized_library_files(
    remote_storage: &GenericRemoteStorage,
    paths: &Vec<RemotePath>,
) -> Result<HashMap<String, Vec<RemotePath>>> {
    let mut library_groups = HashMap::new();
    for file in list_all_files(remote_storage, paths).await? {
        let lib_name = get_library_name(file.get_path().to_str().context("invalid path")?);
        let lib_list = library_groups.entry(lib_name).or_insert(Vec::new());
        lib_list.push(file.to_owned());
    }
    Ok(library_groups)
}

// store a path, paired with a flag indicating whether the path is to a file in
// the root or subdirectory
#[derive(Debug)]
pub struct PathAndFlag {
    path: RemotePath,
    subdir_flag: bool,
}

// get_ext_name extracts the extension name, and returns a flag indicating
// whether this file is in a subdirectory or not.
//
// extension files can be in subdirectories of the extension store.
// examples of layout:
// v14//share//extension/extension_name--1.0.sql,
// v14//share//extension/extension_name/extension_name--1.0.sql,
// v14//share//extension/extension_name/extra_data.csv
// Note: we *assume* that the  extension files is in one of these formats.
// If it is not, this code's behavior is *undefined*.
fn get_ext_name(path: &str) -> Result<(&str, bool)> {
    let path_suffix: Vec<&str> = path.split(&format!("{SHARE_EXT_PATH}/")).collect();
    let ext_name = path_suffix.last().expect("bad ext name");

    if let Some(index) = ext_name.find('/') {
        return Ok((&ext_name[..index], true));
    } else if let Some(index) = ext_name.find("--") {
        return Ok((&ext_name[..index], false));
    }
    Ok((ext_name, false))
}

// helper to collect files of given prefixes for extensions and group them by extension
// returns a hashmap of (extension_name, Vector of remote paths for all files needed for this extension)
// and a list of control files
// For example, an entry in the hashmap could be
// {"anon": [RemotePath("v14/anon/share/extension/anon/address.csv"),
// RemotePath("v14/anon/share/extension/anon/anon--1.1.0.sql")]},
// with corresponding list of control files entry being
// {"anon.control": RemotePath("v14/anon/share/extension/anon.control")}
async fn organized_extension_files(
    remote_storage: &GenericRemoteStorage,
    paths: &Vec<RemotePath>,
) -> Result<(HashMap<String, Vec<PathAndFlag>>, Vec<RemotePath>)> {
    let mut grouped_dependencies = HashMap::new();
    let mut control_files = Vec::new();

    for file in list_all_files(remote_storage, paths).await? {
        if file.extension().context("bad file name")? == "control" {
            control_files.push(file.to_owned());
        } else {
            let (file_ext_name, subdir_flag) =
                get_ext_name(file.get_path().to_str().context("invalid path")?)?;
            let ext_file_list = grouped_dependencies
                .entry(file_ext_name.to_string())
                .or_insert(Vec::new());
            ext_file_list.push(PathAndFlag {
                path: file.to_owned(),
                subdir_flag,
            });
        }
    }
    Ok((grouped_dependencies, control_files))
}
