// AT LEAST download_extension / get_available_extensions need large changes
// refactor custom_extensions to be clear that we have moved the access logic to control plane

// Download extension files from the extension store
// and put them in the right place in the postgres directory
/*
The layout of the S3 bucket is as follows:
├── 111
    │   ├── v14
    │   │   ├── extensions
    │   │   │   ├── anon.tar.zst
    │   │   │   └── embedding.tar.zst
    │   │   └── ext_index.json
    │   └── v15
    │       ├── extensions
    │       │   ├── anon.tar.zst
    │       │   └── embedding.tar.zst
    │       └── ext_index.json
    ├── 112
    │   ├── v14
    │   │   ├── extensions
    │   │   │   └── anon.tar.zst
    │   │   └── ext_index.json
    │   └── v15
    │       ├── extensions
    │       │   └── anon.tar.zst
    │       └── ext_index.json
    └── 113
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

We do not duplicate extension.tar.zst files.
We only upload a new one if it is updated.
*access* is controlled by spec

More specifically, here is an example ext_index.json
{
  "embedding": {
    "control_file_content": "comment = 'hnsw index' \ndefault_version = '0.1.0' \nmodule_pathname = '$libdir/embedding' \nrelocatable = true \ntrusted = true",
    "extension_archive": "111/v14/extensions/embedding.tar.zst"
  },
  "anon": {
    "control_file_content": "# PostgreSQL Anonymizer (anon) extension \ncomment = 'Data anonymization tools' \ndefault_version = '1.1.0' \ndirectory='extension/anon' \nrelocatable = false \nrequires = 'pgcrypto' \nsuperuser = false \nmodule_pathname = '$libdir/anon' \ntrusted = true \n",
    "extension_archive": "111/v14/extensions/anon.tar.zst"
  }
}
*/
use anyhow::Context;
use anyhow::{self, Result};
use futures::future::join_all;
use remote_storage::*;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::io::Read;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::str;
use tar::Archive;
use tokio::io::AsyncReadExt;
use tracing::info;
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
// return the paths in s3 to the archives containing the actual extension files
// for use in creating the extension
pub async fn get_available_extensions(
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
    pg_version: &str,
    enabled_extensions: &[String],
    build_tag: &str,
) -> Result<HashMap<String, RemotePath>> {
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
    let ext_index_str = str::from_utf8(&ext_idx_buffer).expect("error parsing json");
    let ext_index_full: Value = serde_json::from_str(ext_index_str)?;
    let ext_index_full = ext_index_full.as_object().context("error parsing json")?;
    info!("ext_index: {:?}", &ext_index_full);

    info!("enabled_extensions: {:?}", enabled_extensions);
    let mut ext_remote_paths = HashMap::new();
    let mut file_create_tasks = Vec::new();
    for extension in enabled_extensions {
        let ext_data = ext_index_full[extension]
            .as_object()
            .context("error parsing json")?;
        let control_contents = ext_data["control_file_content"]
            .as_str()
            .context("error parsing json")?;
        let control_path = local_sharedir.join(extension.to_owned() + ".control");
        info!("writing file {:?}{:?}", control_path, control_contents);
        file_create_tasks.push(tokio::fs::write(control_path, control_contents));

        let ext_archive_path = ext_data["extension_archive"]
            .as_str()
            .context("error parsing json")?;
        ext_remote_paths.insert(
            extension.to_string(),
            RemotePath::from_string(ext_archive_path)?,
        );
    }
    let results = join_all(file_create_tasks).await;
    for result in results {
        result?;
    }
    Ok(ext_remote_paths)
}

// download all sqlfiles (and possibly data files) for a given extension name
pub async fn download_extension(
    ext_name: &str,
    ext_path: &RemotePath,
    remote_storage: &GenericRemoteStorage,
    pgbin: &str,
) -> Result<()> {
    info!("Download extension {:?} from {:?}", ext_name, ext_path);
    let mut download = remote_storage.download(ext_path).await?;
    let mut download_buffer = Vec::new();
    download
        .download_stream
        .read_to_end(&mut download_buffer)
        .await?;
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
        format!("{unzip_dest}/{ext_name}/share/extension"),
        Path::new(&get_pg_config("--sharedir", pgbin)).join("extension"),
    );
    let libdir_paths = (
        format!("{unzip_dest}/{ext_name}/lib"),
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
            std::fs::rename(old_file, new_file)?;
        }
    }
    Ok(())
}

// This function initializes the necessary structs to use remote storage (should be fairly cheap)
pub fn init_remote_storage(remote_ext_config: &str) -> anyhow::Result<GenericRemoteStorage> {
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
        .unwrap_or_default()
        .to_string();

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
