use std::num::NonZeroUsize;
use std::path::Path;

use aws_sdk_s3::Region;
use s3_deleter::cloud_admin_api::CloudAdminApiClient;
use s3_deleter::{
    get_cloud_admin_api_token_or_exit, init_logging, S3Deleter, TenantsToClean, TEST_BASE_URL,
};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let cloud_admin_api_client =
        CloudAdminApiClient::new(get_cloud_admin_api_token_or_exit(), TEST_BASE_URL.parse()?);

    let file_dir = Path::new("/Users/someonetoignore/Downloads/staging_removals/");
    let tenants_to_clean = TenantsToClean::new(file_dir).await?;
    let tenant_count = tenants_to_clean.tenant_count();
    let s3_deleter = S3Deleter::new(
        Region::from_static("us-east-2"),
        // Region::from_static("eu-west-1"),
        NonZeroUsize::new(15).unwrap(),
        tenants_to_clean,
        cloud_admin_api_client,
    );

    info!("Starting S3 removal, tenants to remove from S3: {tenant_count}");
    s3_deleter.remove_all().await?;
    info!("Finished S3 removal");

    Ok(())
}
