/* 
**WIP**
 * This is a MWE of using our RemoteStorage API to call the aws stuff and download multiple files
 * TODO: s3_bucket some work; for example to make sure the pagination thing goes fine.
macro_rules! alek { ($expression:expr) => { println!("{:?}", $expression); }; }
*/

use remote_storage::*;
use std::path::Path;
use std::fs::File;
use std::io::{BufWriter, Write};
use toml_edit;
use anyhow::{self, Context};
use tokio::io::AsyncReadExt;
use tracing::*;
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let cfg_file_path = Path::new("./../.neon/pageserver.toml");
    let cfg_file_contents = std::fs::read_to_string(cfg_file_path)
    .with_context(|| format!( "Failed to read pageserver config at '{}'", cfg_file_path.display()))?;
    let toml = cfg_file_contents
        .parse::<toml_edit::Document>()
        .with_context(|| format!( "Failed to parse '{}' as pageserver config", cfg_file_path.display()))?;
    let remote_storage_data = toml.get("remote_storage")
        .context("field should be present")?;
    let remote_storage_config = RemoteStorageConfig::from_toml(remote_storage_data)?
        .context("error configuring remote storage")?;
    info!("{:?}", remote_storage_config);
    let remote_storage = GenericRemoteStorage::from_config(&remote_storage_config)?;

    let folder = RemotePath::new(Path::new("public_extensions"))?;
    let from_paths = remote_storage.list_files(Some(&folder)).await?;

    for remote_from_path in from_paths {
        // TODO: where should we actually save the files to?
        if remote_from_path.extension() == Some("control") {
            let file_name = remote_from_path.object_name().expect("it must exist");
            println!("{:?}",file_name);
            let mut download = remote_storage.download(&remote_from_path).await?;
            let mut write_data_buffer = Vec::new(); 
            download.download_stream.read_to_end(&mut write_data_buffer).await?;
            let mut output_file = BufWriter::new(File::create(file_name)?);
            output_file.write_all(&mut write_data_buffer)?;
        }
    }

    Ok(())
}
