use std::env;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Region;
use reqwest::Url;
use s3_deleter::cloud_admin_api::CloudAdminApiClient;
use s3_deleter::delete_batch_producer::DeleteBatchProducer;
use s3_deleter::{
    checks, get_cloud_admin_api_token_or_exit, init_logging, init_s3_client, RootTarget, S3Deleter,
    S3Target,
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

    info!("Starting extra tenant S3 removal in bucket {bucket_param}, region {region_param} for node kind '{node_kind}'");
    let cloud_admin_api_client = Arc::new(CloudAdminApiClient::new(
        get_cloud_admin_api_token_or_exit(),
        cloud_admin_api_url_param,
    ));

    let bucket_region = Region::new(region_param);
    let delimiter = "/".to_string();
    let s3_client = Arc::new(init_s3_client(sso_account_id_param, bucket_region));
    let s3_root = match node_kind.trim() {
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
        Arc::clone(&cloud_admin_api_client),
        Arc::clone(&s3_client),
        s3_root.clone(),
        s3_deleter::TraversingDepth::Tenant,
    );

    let s3_deleter = S3Deleter::new(
        dry_run,
        NonZeroUsize::new(15).unwrap(),
        Arc::clone(&s3_client),
        delete_batch_producer.subscribe(),
        s3_root.clone(),
    );

    let (deleter_task_result, batch_producer_task_result) =
        tokio::join!(s3_deleter.remove_all(), delete_batch_producer.join());

    let deletion_stats = deleter_task_result.context("s3 deletion")?;
    info!(
        "Deleted {} tenants ({} keys) abd {} timelines ({} keys) total. Dry run: {}",
        deletion_stats.deleted_tenant_keys.len(),
        deletion_stats.deleted_tenant_keys.values().sum::<usize>(),
        deletion_stats.deleted_timeline_keys.len(),
        deletion_stats.deleted_timeline_keys.values().sum::<usize>(),
        dry_run,
    );
    info!("Total stats: {deletion_stats:?}");

    let batch_producer_stats = batch_producer_task_result.context("delete batch producer join")?;
    info!(
        "Total bucket tenants listed: {}, timelines: {}",
        batch_producer_stats.tenants_checked(),
        batch_producer_stats.timelines_checked(),
    );

    info!("Finished S3 removal");

    if "pageserver" == node_kind.trim() {
        info!("validating active tenants and timelines for pageserver S3 data");

        // TODO kb real stats for validation + better stats for every place: add and print `min`, `max`, `mean` values at least
        let validation_stats = checks::validate_pageserver_active_tenant_and_timelines(
            s3_client,
            s3_root,
            cloud_admin_api_client,
            batch_producer_stats,
        )
        .await
        .context("active tenant and timeline validation")?;
        info!("Finished active tenant and timeline validation, correct timelines: {}, timeline validation errors: {:?}",
            validation_stats.normal_timelines.len(), validation_stats.timelines_with_errors);
    }

    Ok(())
}
