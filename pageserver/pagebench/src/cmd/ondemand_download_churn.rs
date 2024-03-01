use pageserver_api::shard::TenantShardId;

use rand::seq::SliceRandom;
use utils::id::TenantTimelineId;

use tokio::task::JoinSet;

use std::sync::Arc;

/// Evict & on-demand download random layers.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    /// Probability for sending `latest=true` in the request (uniform distribution).
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
    /// Before starting the benchmark, live-reconfigure the pageserver to use the given
    /// [`pageserver_api::models::virtual_file::IoEngineKind`].
    #[clap(long)]
    set_io_engine: Option<pageserver_api::models::virtual_file::IoEngineKind>,
    targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let task = rt.spawn(main_impl(args));
    rt.block_on(task).unwrap().unwrap();
    Ok(())
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    if let Some(engine_str) = &args.set_io_engine {
        mgmt_api_client.put_io_engine(engine_str).await?;
    }

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: args.limit_to_first_n_targets,
            targets: args.targets.clone(),
        },
    )
    .await?;

    let mut tasks = JoinSet::new();
    for tl in timelines {
        tasks.spawn(timeline_task(Arc::clone(&mgmt_api_client), tl));
    }

    while let Some(res) = tasks.join_next().await {
        res.unwrap();
    }
    Ok(())
}

async fn timeline_task(
    mgmt_api_client: Arc<pageserver_client::mgmt_api::Client>,
    timeline: TenantTimelineId,
) {
    // TODO: support sharding
    let tenant_shard_id = TenantShardId::unsharded(timeline.tenant_id);

    let mut layers = mgmt_api_client
        .layer_map_info(tenant_shard_id, timeline.timeline_id)
        .await
        .unwrap();

    loop {
        let layer = {
            let mut rng = rand::thread_rng();
            layers
                .historic_layers
                .choose_mut(&mut rng)
                .expect("no layers")
        };
        enum Action {
            Evict,
            OnDemandDownload,
        }
        let action = if layer.is_remote() {
            Action::OnDemandDownload
        } else {
            Action::Evict
        };
        match action {
            Action::Evict => mgmt_api_client
                .layer_evict(
                    tenant_shard_id,
                    timeline.timeline_id,
                    layer.layer_file_name(),
                )
                .await
                .unwrap(),
            Action::OnDemandDownload => mgmt_api_client
                .layer_ondemand_download(
                    tenant_shard_id,
                    timeline.timeline_id,
                    layer.layer_file_name(),
                )
                .await
                .unwrap(),
        }
        layer.set_remote(layer.is_remote());
    }
}
