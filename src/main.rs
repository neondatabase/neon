use std::env;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Region;
use reqwest::Url;
use s3_deleter::cloud_admin_api::CloudAdminApiClient;
use s3_deleter::delete_batch_producer::DeleteBatchProducer;
use s3_deleter::{
    get_cloud_admin_api_token_or_exit, init_logging, init_s3_client, S3Deleter, S3Target,
};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dry_run = !env::args().skip(1).any(|arg| arg == "--delete");

    let _guard = init_logging(dry_run);
    if dry_run {
        info!("Dry run, not removing items for real");
    } else {
        warn!("Dry run disabled, removing bucket items for real");
    }

    let sso_account_id_param =
        env::var("SSO_ACCOUNT_ID").context("'SSO_ACCOUNT_ID' param retrieval")?;
    let region_param = env::var("REGION").context("'REGION' param retrieval")?;
    let bucket_param = env::var("BUCKET").context("'BUCKET' param retrieval")?;
    let tenant_limit_param: Option<usize> = env::var("TENANT_LIMIT")
        .ok()
        .map(|limit_str| limit_str.parse().context("'limit_str' param parsing"))
        .transpose()?;
    let cloud_admin_api_url_param: Url = env::var("CLOUD_ADMIN_API_URL")
        .context("'CLOUD_ADMIN_API_URL' param retrieval")?
        .parse()
        .context("'CLOUD_ADMIN_API_URL' param parsing")?;

    info!("Starting S3 removal in bucket {bucket_param}, region {region_param}");
    let cloud_admin_api_client = CloudAdminApiClient::new(
        get_cloud_admin_api_token_or_exit(),
        cloud_admin_api_url_param,
    );

    let bucket_region = Region::new(region_param);
    let s3_client = Arc::new(init_s3_client(sso_account_id_param, bucket_region));
    let delimiter = "/".to_string();
    let s3_target = S3Target {
        bucket_name: bucket_param,
        prefix_in_bucket: ["pageserver", "v1", "tenants", ""].join(&delimiter),
        delimiter,
    };

    let delete_batch_producer = DeleteBatchProducer::start(
        cloud_admin_api_client,
        Arc::clone(&s3_client),
        s3_target.clone(),
        tenant_limit_param,
    );

    let s3_deleter = S3Deleter::new(
        dry_run,
        NonZeroUsize::new(15).unwrap(),
        s3_client,
        delete_batch_producer.subscribe(),
        s3_target,
    );

    let (deleter_task_result, batch_producer_task_result) =
        tokio::join!(s3_deleter.remove_all(), delete_batch_producer.join());

    let deletion_stats = deleter_task_result.context("s3 deletion")?;
    info!(
        "Deleted {} tenants and {} keys total. Dry run: {}",
        deletion_stats.len(),
        deletion_stats.values().sum::<usize>(),
        dry_run,
    );
    info!("Total stats: {deletion_stats:?}");

    let total_tenants_listed = batch_producer_task_result.context("delete batch producer join")?;
    info!("Total bucket tenants listed: {total_tenants_listed}");

    info!("Finished S3 removal");

    Ok(())
}
