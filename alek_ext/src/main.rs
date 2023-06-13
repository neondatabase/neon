/* 
**WIP**
 * This is a MWE of using our RemoteStorage API to call the aws stuff and download multiple files
 * TODO:
 * 2. s3_bucket needs a lot of work; for example to make sure the pagination thing goes fine.
 * 3. figure out how to write directly from the buffer to a local file; and just generally clean up main.rs
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
    let cfg_file_contents = std::fs::read_to_string(cfg_file_path).unwrap();
    let toml = cfg_file_contents
        .parse::<toml_edit::Document>()
        .context("Error parsing toml")?;
    let remote_storage_data = toml.get("remote_storage")
        .expect("field should be present");
    let remote_storage_config = RemoteStorageConfig::from_toml(remote_storage_data)
        .expect("error parsing toml")
        .expect("error parsing toml");
    info!("{:?}", remote_storage_config);
    let remote_storage = GenericRemoteStorage::from_config(&remote_storage_config)?;

    let folder = RemotePath::new(Path::new("public_extensions"))?;
    let from_paths = remote_storage.list_files(Some(&folder)).await?;

    let mut i = 0;
    for remote_from_path in from_paths {
        i += 1;
        println!("number {i}{:?}", remote_from_path.object_name());
        if remote_from_path.extension() == Some("control") {
            let mut data = remote_storage.download(&remote_from_path).await?;
            // write `data` to a file locally
            // TODO: I think that the way I'm doing this is not optimal;
            // It should be possible to write the data directly to a file
            // rather than first writing it to a vector...
            let mut write_data_buffer = Vec::new(); 
            data.download_stream.read_to_end(&mut write_data_buffer).await?;
            let f = File::create(format!("alek{i}.out")).expect("problem creating file");
            let mut f = BufWriter::new(f);
            f.write_all(&mut write_data_buffer).expect("error writing data");
        }
    }

    Ok(())
}
