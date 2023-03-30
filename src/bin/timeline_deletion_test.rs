use std::env;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Region;
use reqwest::Url;
use s3_deleter::cloud_admin_api::CloudAdminApiClient;
use s3_deleter::delete_batch_producer::{DeleteBatch, DeleteBatchProducer};
use s3_deleter::{
    get_cloud_admin_api_token_or_exit, init_logging, init_s3_client, RootTarget, S3Target,
    TraversingDepth,
};
use tracing::{info, info_span, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args();
    let binary_name = args
        .next()
        .context("binary name in not the first argument")?;
    let dry_run = !args.any(|arg| arg == "--delete");
    let _guard = init_logging(dry_run);

    let _main_span = info_span!("main", binary = %binary_name, %dry_run).entered();
    if dry_run {
        info!("Dry run, not removing items for real");
    } else {
        warn!("Dry run disabled, removing bucket items for real");
    }

    let sso_account_id_param =
        env::var("SSO_ACCOUNT_ID").context("'SSO_ACCOUNT_ID' param retrieval")?;
    let region_param = env::var("REGION").context("'REGION' param retrieval")?;
    let bucket_param = env::var("BUCKET").context("'BUCKET' param retrieval")?;
    let cloud_admin_api_url_param: Url = env::var("CLOUD_ADMIN_API_URL")
        .context("'CLOUD_ADMIN_API_URL' param retrieval")?
        .parse()
        .context("'CLOUD_ADMIN_API_URL' param parsing")?;

    let mut node_kind = env::var("NODE_KIND").context("'NODE_KIND' param retrieval")?;
    node_kind.make_ascii_lowercase();

    // TODO kb env arg + parsing
    let traversing_depth = TraversingDepth::Timeline;

    info!("Starting extra S3 removal in bucket {bucket_param}, region {region_param} for node kind '{node_kind}', traversing depth: {traversing_depth:?}");
    let cloud_admin_api_client = CloudAdminApiClient::new(
        get_cloud_admin_api_token_or_exit(),
        cloud_admin_api_url_param,
    );

    let bucket_region = Region::new(region_param);
    let s3_client = Arc::new(init_s3_client(sso_account_id_param, bucket_region));
    let delimiter = "/".to_string();
    let s3_target = match node_kind.trim() {
        "pageserver" => RootTarget::Pageserver(S3Target {
            bucket_name: bucket_param,
            prefix_in_bucket: ["pageserver", "v1", "tenants", ""].join(&delimiter),
            delimiter,
        }),
        "safekeeper" => RootTarget::Safekeeper(S3Target {
            bucket_name: bucket_param,
            prefix_in_bucket: ["safekeeper", "v1", "wal", ""].join(&delimiter),
            delimiter,
        }),
        unknown => anyhow::bail!("Unknown node type {unknown}"),
    };

    let delete_batch_producer = DeleteBatchProducer::start(
        cloud_admin_api_client,
        Arc::clone(&s3_client),
        s3_target,
        traversing_depth,
    );

    let delete_batch_receiver = delete_batch_producer.subscribe();
    let batch_items_peeker = async move {
        let mut all_items = DeleteBatch::default();
        let mut batch_subscription = delete_batch_receiver.lock().await;
        while let Some(new_batch) = batch_subscription.recv().await {
            all_items.merge(new_batch);
        }
        all_items
    };

    let (all_items_to_delete, batch_producer_task_result) =
        tokio::join!(batch_items_peeker, delete_batch_producer.join());
    info!(
        "{} tenants and {} timelines should be deleted",
        all_items_to_delete.tenants.len(),
        all_items_to_delete.timelines.len()
    );
    let batch_producer_stats = batch_producer_task_result.context("delete batch producer join")?;
    info!(
        "Total bucket tenants listed: {}, timelines: {}",
        batch_producer_stats.tenants_checked(),
        batch_producer_stats.timelines_checked()
    );
    assert!(
        all_items_to_delete.tenants.len() + all_items_to_delete.timelines.len()
            <= batch_producer_stats.tenants_checked() + batch_producer_stats.timelines_checked()
    );
    info!("Finished S3 removal");

    Ok(())
}
