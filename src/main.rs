use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Region;
use s3_deleter::cloud_admin_api::CloudAdminApiClient;
use s3_deleter::delete_batch_producer::DeleteBatchProducer;
use s3_deleter::{
    get_cloud_admin_api_token_or_exit, init_logging, init_s3_client, S3Deleter, S3Target,
    TEST_BASE_URL,
};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = init_logging();

    info!("Starting S3 removal");
    let cloud_admin_api_client =
        CloudAdminApiClient::new(get_cloud_admin_api_token_or_exit(), TEST_BASE_URL.parse()?);

    let bucket_region = Region::from_static("us-east-2");
    let s3_client = Arc::new(init_s3_client(bucket_region));
    let delimiter = "/".to_string();
    let s3_target = S3Target {
        bucket_name: "neon-staging-storage-us-east-2".to_string(),
        prefix_in_bucket: ["pageserver", "v1", "tenants", ""].join(&delimiter),
        delimiter,
    };

    let delete_batch_producer = DeleteBatchProducer::start(
        cloud_admin_api_client,
        Arc::clone(&s3_client),
        s3_target.clone(),
    );

    let dry_run = true;
    let s3_deleter = S3Deleter::new(
        dry_run,
        NonZeroUsize::new(15).unwrap(),
        s3_client,
        delete_batch_producer.subscribe(),
        s3_target,
    );

    let (deleter_task_result, batch_producer_task_result) =
        tokio::join!(s3_deleter.remove_all(), delete_batch_producer.join());

    deleter_task_result.context("s3 deletion")?;
    batch_producer_task_result.context("delete batch producer join")?;

    info!("Finished S3 removal");

    Ok(())
}
