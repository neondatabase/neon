/*
 * This is a MWE of using the aws-sdk-s3 to download a file from an S3 bucket 
 * */

use aws_sdk_s3::{self, config::Region, Error};
use aws_config::{self, meta::region::RegionProviderChain};


#[tokio::main]
async fn main() -> Result<(), Error> {
    let region_provider = RegionProviderChain::first_try(Region::new("eu-central-1"))
        .or_default_provider()
        .or_else(Region::new("eu-central-1"));

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = aws_sdk_s3::Client::new(&shared_config);

    let bucket_name = "neon-dev-extensions";
    let object_key = "fuzzystrmatch.control";
    let response = client
        .get_object()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await?;

    let stuff = response.body;
    let data = stuff.collect().await.expect("error reading data").to_vec();
    println!("data: {:?}", std::str::from_utf8(&data));

    Ok(())
}

