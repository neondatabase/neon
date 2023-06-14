/* 
**WIP**
 * This is a MWE of using our RemoteStorage API to call the aws stuff and download multiple files
*/

#![allow(unused_imports)]
use remote_storage::*;
use std::path::Path;
use std::fs::File;
use std::io::{BufWriter, Write};
use toml_edit;
use anyhow::{self, Context};
use tokio::io::AsyncReadExt;                                  

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    /* me trying to hack RemotePath into submission */

    let cfg_file_path = Path::new("./../.neon/pageserver.toml");
    let cfg_file_contents = std::fs::read_to_string(cfg_file_path)
        .expect("couldn't find pageserver.toml; make sure you are in neon/alek_ext");
    let toml = cfg_file_contents
        .parse::<toml_edit::Document>()
        .expect("Error parsing toml");
    let remote_storage_data = toml.get("remote_storage")
        .expect("field should be present");
    let remote_storage_config = RemoteStorageConfig::from_toml(remote_storage_data)
        .expect("error parsing toml")
        .expect("error parsing toml");
    let remote_storage = GenericRemoteStorage::from_config(&remote_storage_config)?;

    if let GenericRemoteStorage::AwsS3(mybucket) = remote_storage {
        let resp = mybucket
        .client
        .list_objects_v2()
        .bucket("neon-dev-extensions")
        .set_prefix(Some("public_extensions".to_string()))
        .delimiter("/".to_string())
        .send().await?;

        let z = resp.common_prefixes.unwrap();
        for yy in z {
            println!("plzplz: {:?}",yy);
        }
        let mut i = 0;
        for remote_from_path in from_paths {
            i += 1;
            println!("{:?}", &remote_from_path);
            if remote_from_path.extension() == Some("control") {
                let mut data = remote_storage.download(&remote_from_path).await?;
                // write `data` to a file locally
                // TODO: I think that the way I'm doing this is not optimal;
                // It should be possible to write the data directly to a file
                // rather than first writing it to a vector...
                let mut write_data_buffer = Vec::new(); 
                data.download_stream.read_to_end(&mut write_data_buffer).await?;
                let f = File::create("alek{i}.out").expect("problem creating file");
                let mut f = BufWriter::new(f);
                f.write_all(&mut write_data_buffer).expect("error writing data");
            }
        }
    }

    Ok(())
}
