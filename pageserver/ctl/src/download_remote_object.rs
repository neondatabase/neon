use camino::Utf8PathBuf;
use clap::Parser;
use tokio_util::sync::CancellationToken;

/// Download a specific object from remote storage to a local file.
///
/// The remote storage configuration is supplied via the `REMOTE_STORAGE_CONFIG` environment
/// variable, in the same TOML format that the pageserver itself understands. This allows the
/// command to work with any cloud supported by the `remote_storage` crate (currently AWS S3,
/// Azure Blob Storage and local files), as long as the credentials are available via the
/// standard environment variables expected by the underlying SDKs.
///
/// Examples for setting the environment variable:
///
/// ```bash
/// # AWS S3 (region can also be provided via AWS_REGION)
/// export REMOTE_STORAGE_CONFIG='remote_storage = { bucket_name = "my-bucket", bucket_region = "us-east-2" }'
///
/// # Azure Blob Storage (account key picked up from AZURE_STORAGE_ACCOUNT_KEY)
/// export REMOTE_STORAGE_CONFIG='remote_storage = { container = "my-container", account = "my-account" }'
/// ```
#[derive(Parser)]
pub(crate) struct DownloadRemoteObjectCmd {
    /// Key / path of the object to download (relative to the remote storage prefix).
    ///
    /// Examples:
    ///   "wal/3aa8f.../00000001000000000000000A"
    ///   "pageserver/v1/tenants/<tenant_id>/timelines/<timeline_id>/layer_12345"
    pub remote_path: String,

    /// Path of the local file to create. Existing file will be overwritten.
    ///
    /// Examples:
    ///   "./segment"
    ///   "/tmp/layer_12345.parquet"
    pub output_file: Utf8PathBuf,
}

pub(crate) async fn main(cmd: &DownloadRemoteObjectCmd) -> anyhow::Result<()> {
    use remote_storage::{DownloadOpts, GenericRemoteStorage, RemotePath, RemoteStorageConfig};

    // Fetch remote storage configuration from the environment
    let config_str = std::env::var("REMOTE_STORAGE_CONFIG").map_err(|_| {
        anyhow::anyhow!(
            "'REMOTE_STORAGE_CONFIG' environment variable must be set to a valid remote storage TOML config"
        )
    })?;

    let config = RemoteStorageConfig::from_toml_str(&config_str)?;

    // Initialise remote storage client
    let storage = GenericRemoteStorage::from_config(&config).await?;

    // RemotePath must be relative – leading slashes confuse the parser.
    let remote_path_str = cmd.remote_path.trim_start_matches('/');
    let remote_path = RemotePath::from_string(remote_path_str)?;

    let cancel = CancellationToken::new();

    println!(
        "Downloading '{remote_path}' from remote storage bucket {:?} ...",
        config.storage.bucket_name()
    );

    // Start the actual download
    let download = storage
        .download(&remote_path, &DownloadOpts::default(), &cancel)
        .await?;

    // Stream to file
    let mut reader = tokio_util::io::StreamReader::new(download.download_stream);
    let tmp_path = cmd.output_file.with_extension("tmp");
    let mut file = tokio::fs::File::create(&tmp_path).await?;
    tokio::io::copy(&mut reader, &mut file).await?;
    file.sync_all().await?;
    // Atomically move into place
    tokio::fs::rename(&tmp_path, &cmd.output_file).await?;

    println!(
        "Downloaded to '{}'. Last modified: {:?}, etag: {}",
        cmd.output_file, download.last_modified, download.etag
    );

    Ok(())
}
