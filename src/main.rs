use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::config::Region;
use reqwest::Url;
use s3_scrubber::cloud_admin_api::CloudAdminApiClient;
use s3_scrubber::delete_batch_producer::DeleteBatchProducer;
use s3_scrubber::{
    checks, get_cloud_admin_api_token_or_exit, init_logging, init_s3_client, RootTarget, S3Deleter,
    S3Target, TraversingDepth,
};
use tracing::{info, info_span, warn};

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, default_value_t = false)]
    delete: bool,
}

#[derive(ValueEnum, Clone, Copy, Eq, PartialEq)]
enum NodeKind {
    Safekeeper,
    Pageserver,
}

impl NodeKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Safekeeper => "safekeeper",
            Self::Pageserver => "pageserver",
        }
    }
}

impl Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Subcommand)]
enum Command {
    Tidy {
        #[arg(short, long)]
        node_kind: NodeKind,
        #[arg(short, long, default_value_t=TraversingDepth::Tenant)]
        depth: TraversingDepth,
        #[arg(short, long, default_value_t = false)]
        skip_validation: bool,
    },
}

struct BucketConfig {
    region: String,
    bucket: String,
    sso_account_id: String,
}

impl Display for BucketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.sso_account_id, self.region, self.bucket)
    }
}

impl BucketConfig {
    fn from_env() -> anyhow::Result<Self> {
        let sso_account_id =
            env::var("SSO_ACCOUNT_ID").context("'SSO_ACCOUNT_ID' param retrieval")?;
        let region = env::var("REGION").context("'REGION' param retrieval")?;
        let bucket = env::var("BUCKET").context("'BUCKET' param retrieval")?;

        Ok(Self {
            region,
            bucket,
            sso_account_id,
        })
    }
}

struct ConsoleConfig {
    admin_api_url: Url,
}

impl ConsoleConfig {
    fn from_env() -> anyhow::Result<Self> {
        let admin_api_url: Url = env::var("CLOUD_ADMIN_API_URL")
            .context("'CLOUD_ADMIN_API_URL' param retrieval")?
            .parse()
            .context("'CLOUD_ADMIN_API_URL' param parsing")?;

        Ok(Self { admin_api_url })
    }
}

async fn tidy(
    cli: &Cli,
    bucket_config: BucketConfig,
    console_config: ConsoleConfig,
    node_kind: NodeKind,
    depth: TraversingDepth,
    skip_validation: bool,
) -> anyhow::Result<()> {
    let binary_name = env::args()
        .next()
        .context("binary name in not the first argument")?;

    let dry_run = !cli.delete;
    let _guard = init_logging(&binary_name, dry_run, node_kind.as_str());
    let _main_span = info_span!("tidy", binary = %binary_name, %dry_run).entered();

    if dry_run {
        info!("Dry run, not removing items for real");
    } else {
        warn!("Dry run disabled, removing bucket items for real");
    }

    info!("skip_validation={skip_validation}");

    info!("Starting extra S3 removal in {bucket_config} for node kind '{node_kind}', traversing depth: {depth:?}");

    info!("Starting extra tenant S3 removal in {bucket_config} for node kind '{node_kind}'");
    let cloud_admin_api_client = Arc::new(CloudAdminApiClient::new(
        get_cloud_admin_api_token_or_exit(),
        console_config.admin_api_url,
    ));

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

    match &cli.command {
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
                *node_kind,
                *depth,
                *skip_validation,
            )
            .await
        }
    }
}
