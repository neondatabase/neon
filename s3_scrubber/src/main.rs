use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::config::Region;
use s3_scrubber::cloud_admin_api::CloudAdminApiClient;
use s3_scrubber::delete_batch_producer::DeleteBatchProducer;
use s3_scrubber::garbage::{find_garbage, purge_garbage, PurgeMode};
use s3_scrubber::scan_metadata::scan_metadata;
use s3_scrubber::{
    checks, init_logging, init_s3_client, BucketConfig, ConsoleConfig, NodeKind, RootTarget,
    S3Deleter, S3Target, TraversingDepth,
};
use tracing::{info, warn};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, default_value_t = false)]
    delete: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    Tidy {
        #[arg(short, long)]
        node_kind: NodeKind,
        #[arg(short, long, default_value_t=TraversingDepth::Tenant)]
        depth: TraversingDepth,
        #[arg(short, long, default_value_t = false)]
        skip_validation: bool,
    },
    FindGarbage {
        #[arg(short, long)]
        node_kind: NodeKind,
        #[arg(short, long, default_value_t=TraversingDepth::Tenant)]
        depth: TraversingDepth,
        #[arg(short, long, default_value_t = String::from("garbage.json"))]
        output_path: String,
    },
    PurgeGarbage {
        #[arg(short, long)]
        input_path: String,
        #[arg(short, long, default_value_t = PurgeMode::DeletedOnly)]
        mode: PurgeMode,
    },
    ScanMetadata {},
}

async fn tidy(
    cli: &Cli,
    bucket_config: BucketConfig,
    console_config: ConsoleConfig,
    node_kind: NodeKind,
    depth: TraversingDepth,
    skip_validation: bool,
) -> anyhow::Result<()> {
    let dry_run = !cli.delete;
    if dry_run {
        info!("Dry run, not removing items for real");
    } else {
        warn!("Dry run disabled, removing bucket items for real");
    }

    info!("skip_validation={skip_validation}");

    info!("Starting extra S3 removal in {bucket_config} for node kind '{node_kind}', traversing depth: {depth:?}");

    info!("Starting extra tenant S3 removal in {bucket_config} for node kind '{node_kind}'");
    let cloud_admin_api_client = Arc::new(CloudAdminApiClient::new(console_config));

    let bucket_region = Region::new(bucket_config.region);
    let delimiter = "/".to_string();
    let s3_client = Arc::new(init_s3_client(bucket_config.sso_account_id, bucket_region));
    let s3_root = match node_kind {
        NodeKind::Pageserver => RootTarget::Pageserver(S3Target {
            bucket_name: bucket_config.bucket,
            prefix_in_bucket: ["pageserver", "v1", "tenants", ""].join(&delimiter),
            delimiter,
        }),
        NodeKind::Safekeeper => RootTarget::Safekeeper(S3Target {
            bucket_name: bucket_config.bucket,
            prefix_in_bucket: ["safekeeper", "v1", "wal", ""].join(&delimiter),
            delimiter,
        }),
    };

    let delete_batch_producer = DeleteBatchProducer::start(
        Arc::clone(&cloud_admin_api_client),
        Arc::clone(&s3_client),
        s3_root.clone(),
        depth,
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
        "Deleted {} tenants ({} keys) and {} timelines ({} keys) total. Dry run: {}",
        deletion_stats.deleted_tenant_keys.len(),
        deletion_stats.deleted_tenant_keys.values().sum::<usize>(),
        deletion_stats.deleted_timeline_keys.len(),
        deletion_stats.deleted_timeline_keys.values().sum::<usize>(),
        dry_run,
    );
    info!(
        "Total tenant deletion stats: {:?}",
        deletion_stats
            .deleted_tenant_keys
            .into_iter()
            .map(|(id, key)| (id.to_string(), key))
            .collect::<HashMap<_, _>>()
    );
    info!(
        "Total timeline deletion stats: {:?}",
        deletion_stats
            .deleted_timeline_keys
            .into_iter()
            .map(|(id, key)| (id.to_string(), key))
            .collect::<HashMap<_, _>>()
    );

    let batch_producer_stats = batch_producer_task_result.context("delete batch producer join")?;
    info!(
        "Total bucket tenants listed: {}; for {} active tenants, timelines checked: {}",
        batch_producer_stats.tenants_checked(),
        batch_producer_stats.active_tenants(),
        batch_producer_stats.timelines_checked()
    );

    if node_kind == NodeKind::Pageserver {
        info!("node_kind != pageserver, finish without performing validation step");
        return Ok(());
    }

    if skip_validation {
        info!("--skip-validation is set, exiting");
        return Ok(());
    }

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
    info!("Finished active tenant and timeline validation, correct timelines: {}, timeline validation errors: {}",
        validation_stats.normal_timelines.len(), validation_stats.timelines_with_errors.len());
    if !validation_stats.timelines_with_errors.is_empty() {
        warn!(
            "Validation errors: {:#?}",
            validation_stats
                .timelines_with_errors
                .into_iter()
                .map(|(id, errors)| (id.to_string(), format!("{errors:?}")))
                .collect::<HashMap<_, _>>()
        );
    }

    info!("Done");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let bucket_config = BucketConfig::from_env()?;

    let command_log_name = match &cli.command {
        Command::ScanMetadata { .. } => "scan",
        Command::FindGarbage { .. } => "find-garbage",
        Command::PurgeGarbage { .. } => "purge-garbage",
        Command::Tidy { .. } => "tidy",
    };
    let _guard = init_logging(&format!(
        "{}_{}_{}_{}.log",
        std::env::args().next().unwrap(),
        command_log_name,
        bucket_config.bucket,
        chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S")
    ));

    match cli.command {
        Command::Tidy {
            node_kind,
            depth,
            skip_validation,
        } => {
            let console_config = ConsoleConfig::from_env()?;
            tidy(
                &cli,
                bucket_config,
                console_config,
                node_kind,
                depth,
                skip_validation,
            )
            .await
        }
        Command::ScanMetadata {} => match scan_metadata(bucket_config).await {
            Err(e) => {
                tracing::error!("Failed: {e}");
                Err(e)
            }
            Ok(summary) => {
                println!("{}", summary.summary_string());
                if summary.is_fatal() {
                    Err(anyhow::anyhow!("Fatal scrub errors detected"))
                } else {
                    Ok(())
                }
            }
        },
        Command::FindGarbage {
            node_kind,
            depth,
            output_path,
        } => {
            let console_config = ConsoleConfig::from_env()?;
            find_garbage(bucket_config, console_config, depth, node_kind, output_path).await
        }
        Command::PurgeGarbage { input_path, mode } => {
            purge_garbage(input_path, mode, !cli.delete).await
        }
    }
}
