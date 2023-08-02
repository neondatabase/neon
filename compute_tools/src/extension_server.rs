// Download extension files from the extension store
// and put them in the right place in the postgres directory (share / lib)
/*
The layout of the S3 bucket is as follows:
5615610098 // this is an extension build number
├── v14
│   ├── extensions
│   │   ├── anon.tar.zst
│   │   └── embedding.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   ├── anon.tar.zst
    │   └── embedding.tar.zst
    └── ext_index.json
5615261079
├── v14
│   ├── extensions
│   │   └── anon.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   └── anon.tar.zst
    └── ext_index.json
5623261088
├── v14
│   ├── extensions
│   │   └── embedding.tar.zst
│   └── ext_index.json
└── v15
    ├── extensions
    │   └── embedding.tar.zst
    └── ext_index.json

Note that build number cannot be part of prefix because we might need extensions
from other build numbers.

ext_index.json stores the control files and location of extension archives
It also stores a list of public extensions and a library_index

We don't need to duplicate extension.tar.zst files.
We only need to upload a new one if it is updated.
(Although currently we just upload every time anyways, hopefully will change
this sometime)

*access* is controlled by spec

More specifically, here is an example ext_index.json
{
    "public_extensions": [
        "anon",
        "pg_buffercache"
    ],
    "library_index": {
        "anon": "anon",
        "pg_buffercache": "pg_buffercache"
    },
    "extension_data": {
        "pg_buffercache": {
            "control_data": {
                "pg_buffercache.control": "# pg_buffercache extension \ncomment = 'examine the shared buffer cache' \ndefault_version = '1.3' \nmodule_pathname = '$libdir/pg_buffercache' \nrelocatable = true \ntrusted=true"
            },
            "archive_path": "5670669815/v14/extensions/pg_buffercache.tar.zst"
        },
        "anon": {
            "control_data": {
                "anon.control": "# PostgreSQL Anonymizer (anon) extension \ncomment = 'Data anonymization tools' \ndefault_version = '1.1.0' \ndirectory='extension/anon' \nrelocatable = false \nrequires = 'pgcrypto' \nsuperuser = false \nmodule_pathname = '$libdir/anon' \ntrusted = true \n"
            },
            "archive_path": "5670669815/v14/extensions/anon.tar.zst"
        }
    }
}
*/
use anyhow::Context;
use anyhow::{self, Result};
use futures::future::join_all;
use remote_storage::*;
use serde_json;
use std::collections::HashMap;
use std::io::Read;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use tar::Archive;
use tokio::io::AsyncReadExt;
use tracing::info;
use tracing::log::warn;
use zstd::stream::read::Decoder;

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

// download control files for enabled_extensions
// return Hashmaps converting library names to extension names (library_index)
// and specifying the remote path to the archive for each extension name
pub async fn get_available_extensions(
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    pg_version: &str,
    custom_extensions: &[String],
    build_tag: &str,
) -> Result<(HashMap<String, RemotePath>, HashMap<String, String>)> {
    let local_sharedir = Path::new(&get_pg_config("--sharedir", pgbin)).join("extension");
    let index_path = format!("{build_tag}/{pg_version}/ext_index.json");
    let index_path = RemotePath::new(Path::new(&index_path)).context("error forming path")?;
    info!("download ext_index.json from: {:?}", &index_path);

    let mut download = remote_storage.download(&index_path).await?;
    let mut ext_idx_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut ext_idx_buffer)
        .await?;
    info!("ext_index downloaded");

    #[derive(Debug, serde::Deserialize)]
    struct Index {
        public_extensions: Vec<String>,
        library_index: HashMap<String, String>,
        extension_data: HashMap<String, ExtensionData>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct ExtensionData {
        control_data: HashMap<String, String>,
        archive_path: String,
    }

    let ext_index_full = serde_json::from_slice::<Index>(&ext_idx_buffer)?;
    let mut enabled_extensions = ext_index_full.public_extensions;
    enabled_extensions.extend_from_slice(custom_extensions);
    let library_index = ext_index_full.library_index;
    let all_extension_data = ext_index_full.extension_data;
    info!("library_index: {:?}", library_index);

    info!("enabled_extensions: {:?}", enabled_extensions);
    let mut ext_remote_paths = HashMap::new();
    let mut file_create_tasks = Vec::new();
    for extension in enabled_extensions {
        let ext_data = &all_extension_data[&extension];
        for (control_file, control_contents) in &ext_data.control_data {
            let extension_name = control_file
                .strip_suffix(".control")
                .expect("control files must end in .control");
            ext_remote_paths.insert(
                extension_name.to_string(),
                RemotePath::from_string(&ext_data.archive_path)?,
            );
            let control_path = local_sharedir.join(control_file);
            info!("writing file {:?}{:?}", control_path, control_contents);
            file_create_tasks.push(tokio::fs::write(control_path, control_contents));
        }
    }
    let results = join_all(file_create_tasks).await;
    for result in results {
        result?;
    }
    info!("ext_remote_paths {:?}", ext_remote_paths);
    Ok((ext_remote_paths, library_index))
}

// download the archive for a given extension,
// unzip it, and place files in the appropriate locations (share/lib)
pub async fn download_extension(
    ext_name: &str,
    ext_path: &RemotePath,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
) -> Result<u64> {
    info!("Download extension {:?} from {:?}", ext_name, ext_path);
    let mut download = remote_storage.download(ext_path).await?;
    let mut download_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut download_buffer)
        .await?;
    let download_size = download_buffer.len() as u64;
    // it's unclear whether it is more performant to decompress into memory or not
    // TODO: decompressing into memory can be avoided
    let mut decoder = Decoder::new(download_buffer.as_slice())?;
    let mut decompress_buffer = Vec::new();
    decoder.read_to_end(&mut decompress_buffer)?;
    let mut archive = Archive::new(decompress_buffer.as_slice());
    let unzip_dest = pgbin
        .strip_suffix("/bin/postgres")
        .expect("bad pgbin")
        .to_string()
        + "/download_extensions";
    archive.unpack(&unzip_dest)?;
    info!("Download + unzip {:?} completed successfully", &ext_path);

    let sharedir_paths = (
        unzip_dest.to_string() + "/share/extension",
        Path::new(&get_pg_config("--sharedir", pgbin)).join("extension"),
    );
    let libdir_paths = (
        unzip_dest.to_string() + "/lib",
        Path::new(&get_pg_config("--libdir", pgbin)).join("postgresql"),
    );
    // move contents of the libdir / sharedir in unzipped archive to the correct local paths
    for paths in [sharedir_paths, libdir_paths] {
        let (zip_dir, real_dir) = paths;
        info!("mv {zip_dir:?}/*  {real_dir:?}");
        for file in std::fs::read_dir(zip_dir)? {
            let old_file = file?.path();
            let new_file =
                Path::new(&real_dir).join(old_file.file_name().context("error parsing file")?);
            info!("moving {old_file:?} to {new_file:?}");

            // extension download failed: Directory not empty (os error 39)
            match std::fs::rename(old_file, new_file) {
                Ok(()) => info!("move succeeded"),
                Err(e) => {
                    warn!("move failed, probably because the extension already exists: {e}")
                }
            }
        }
    }
    info!("done moving extension {ext_name}");
    Ok(download_size)
}

// This function initializes the necessary structs to use remote storage
pub fn init_remote_storage(remote_ext_config: &str) -> anyhow::Result<GenericRemoteStorage> {
    #[derive(Debug, serde::Deserialize)]
    struct RemoteExtJson {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        prefix: Option<String>,
    }
    let remote_ext_json = serde_json::from_str::<RemoteExtJson>(remote_ext_config)?;

    let config = S3Config {
        bucket_name: remote_ext_json.bucket,
        bucket_region: remote_ext_json.region,
        prefix_in_bucket: remote_ext_json.prefix,
        endpoint: remote_ext_json.endpoint,
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
