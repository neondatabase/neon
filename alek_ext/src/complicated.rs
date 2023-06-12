/*
 * The following code attempts to actually download a file from the S3 bucket specified in
 * `pageserver.toml` as :
 * ```[remote_storage] 
 * bucket_name = 'neon-dev-extensions'
 * bucket_region = 'eu-central-1'
 *
 * Note: must run `export AWS_PROFILE=PowerUserAccess-11111111111` for your SSO credentials
 * to get loaded; alternatively go get other credentials. But SSO is better.
 *
 * Next steps:
 * 1. **make it work with AWS** (this code hopefully!)
 * 2. make it work for downloading multiple files, not just a single file
 * 3. integrate it with compute_ctl
 * 4. actually upload stuff to the bucket? so that it can be downloaded. Does this allow us to
 *    modify `Dockerfile.computenode`? to delete the extension loading that is happening there?
 * 5. How do the tenants upload extensions?
 * 6. Maybe think about duplicating less stuff. 
 * */

use remote_storage::*;
use std::path::Path;
use std::fs::File;
use std::io::{BufWriter, Write};
use toml_edit;
use anyhow;
use tokio::io::AsyncReadExt;                                  
use remote_storage::GenericRemoteStorage::AwsS3;

async fn download_file() -> anyhow::Result<()> {
    // read configurations from `pageserver.toml`
    let cfg_file_path = Path::new("./../.neon/pageserver.toml");
    let cfg_file_contents = std::fs::read_to_string(cfg_file_path).unwrap();
    let toml = cfg_file_contents
        .parse::<toml_edit::Document>()
        .expect("Error parsing toml");
    let remote_storage_data = toml.get("remote_storage")
        .expect("field should be present");
    let remote_storage_config = RemoteStorageConfig::from_toml(remote_storage_data)
        .expect("error parsing toml")
        .expect("error parsing toml");

    println!("CONFIG LGTM!!!\n {:?}", remote_storage_config);

    // query S3 bucket
    let remote_storage = GenericRemoteStorage::from_config(&remote_storage_config)?;
    let from_path = "neon-dev-extensions/fuzzystrmatch.control";
    let remote_from_path = RemotePath::new(Path::new(from_path))?;
        
    if let AwsS3(printablebucket) = &remote_storage {
        println!("S3Bucket looks fine, AFAICT{:?}", printablebucket);
    }
    println!("{:?}"&remote_from_path);

    let mut data = remote_storage.download(&remote_from_path).await;

    /*
    let mut write_data_buffer = Vec::new(); 

    data.download_stream.read_to_end(&mut write_data_buffer).await?;

    // write `data` to a file locally
    let f = File::create("alek.out").expect("problem creating file");
    let mut f = BufWriter::new(f);
    f.write_all(&mut write_data_buffer).expect("error writing data");
    */

    Ok(())
}

#[tokio::main]
async fn main() {
    match download_file().await {
        Err(_)=>println!("Err"),
        _ => println!("SUCEECESS")
    }
}
