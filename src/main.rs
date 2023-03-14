use std::num::NonZeroUsize;
use std::path::Path;

use aws_sdk_s3::Region;
use s3_deleter::{init_logging, S3Deleter, TenantsToClean};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let file_dir = Path::new("/Users/someonetoignore/Downloads/staging_removals/");
    let tenants_to_clean = TenantsToClean::new(file_dir).await?;
    let tenant_count = tenants_to_clean.tenant_count();
    let s3_deleter = S3Deleter::new(
        Region::from_static("us-east-2"),
        // Region::from_static("eu-west-1"),
        NonZeroUsize::new(15).unwrap(),
        tenants_to_clean,
    );

    info!("Starting S3 removal, tenants to remove from S3: {tenant_count}");
    s3_deleter.remove_all().await?;
    info!("Finished S3 removal");

    Ok(())
}
