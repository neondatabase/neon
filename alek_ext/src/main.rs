use remote_storage::*;
use std::path::Path;
use std::fs::File;
use std::io::{BufWriter, Write};
use toml_edit;
use anyhow;
use tokio::io::AsyncReadExt;                                  

// let region_provider = RegionProviderChain::first_try(Region::new("eu-central-1"))
//     .or_default_provider()
//     .or_else(Region::new("eu-central-1"));

// let shared_config = aws_config::from_env().region(region_provider).load().await;
// let client = aws_sdk_s3::Client::new(&shared_config);

// let bucket_name = "neon-dev-extensions";
// let object_key = "fuzzystrmatch.control";
// let response = client
//     .get_object()
//     .bucket(bucket_name)
//     .key(object_key)
//     .send()
//     .await?;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let from_path = "fuzzystrmatch.control";
    let remote_from_path = RemotePath::new(Path::new(from_path))?;
    println!("{:?}", remote_from_path.clone());

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

    // query S3 bucket
    let remote_storage = GenericRemoteStorage::from_config(&remote_storage_config)?;
    let from_path = "fuzzystrmatch.control";
    let remote_from_path = RemotePath::new(Path::new(from_path))?;
        
    println!("{:?}", remote_from_path.clone());
    // if let GenericRemoteStorage::AwsS3(mybucket) = remote_storage {
    //     println!("{:?}",mybucket.relative_path_to_s3_object(&remote_from_path));
    // }
    let mut data = remote_storage.download(&remote_from_path).await.expect("data yay");
    let mut write_data_buffer = Vec::new(); 
    data.download_stream.read_to_end(&mut write_data_buffer).await?;

    // write `data` to a file locally
    let f = File::create("alek.out").expect("problem creating file");
    let mut f = BufWriter::new(f);
    f.write_all(&mut write_data_buffer).expect("error writing data");

    // let stuff = response.body;
    // let data = stuff.collect().await.expect("error reading data").to_vec();
    // println!("data: {:?}", std::str::from_utf8(&data));

    Ok(())
}
