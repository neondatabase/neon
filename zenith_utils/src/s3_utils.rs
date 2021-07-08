use std::{env, str};

use anyhow::{anyhow, Result};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::{Deserialize, Serialize};
use tokio::runtime;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct S3Storage {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket_name: String,
}

impl S3Storage {
    pub fn new_from_env(bucket_name: &str) -> Result<S3Storage> {
        Ok(S3Storage {
            region: env::var("S3_REGION")?,
            endpoint: env::var("S3_ENDPOINT")?,
            access_key: env::var("S3_ACCESSKEY")?,
            secret_key: env::var("S3_SECRET")?,
            bucket_name: bucket_name.to_string(),
        })
    }

    pub fn get_credentials(&self) -> Result<Credentials> {
        Credentials::new(
            Some(&self.access_key),
            Some(&self.secret_key),
            None,
            None,
            None,
        )
    }

    pub fn get_region(&self) -> Region {
        Region::Custom {
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        }
    }

    pub fn get_bucket(&self) -> Result<Bucket> {
        Bucket::new_with_path_style(
            &self.bucket_name,
            self.get_region(),
            self.get_credentials()?,
        )

        //TODO check if bucket exists and availiable
    }

    pub fn list_bucket(&self, path: String) -> Result<Vec<s3::serde_types::ListBucketResult>> {
        let runtime = runtime::Runtime::new().unwrap();

        let bucket = self.get_bucket()?;
        runtime.block_on(async { bucket.list(path, Some("".to_string())).await })
    }

    pub fn put_object(&self, path: String, content: &[u8]) -> Result<()> {
        let runtime = runtime::Runtime::new().unwrap();

        let bucket = self.get_bucket()?;

        let res = runtime.block_on(async {
            bucket.put_object(path.clone(), content).await
        });

        let (_, retcode) = res?;
        if retcode == 200 { Ok(()) }
        else {
            Err(anyhow!("put_object {} has failed. retcode: {}", path, retcode))
        }
    }

    pub fn get_object(&self, path: &str) -> Result<Vec<u8>> {
        let runtime = runtime::Runtime::new().unwrap();
        let bucket = self.get_bucket()?;

        let res = runtime.block_on(async { bucket.get_object(path).await});

        let (retdata, retcode) = res?;

        if retcode == 200 { Ok(retdata) }
        else {
            Err(anyhow!("get_object {} has failed. retcode: {}", path, retcode))
        }
    }
}
