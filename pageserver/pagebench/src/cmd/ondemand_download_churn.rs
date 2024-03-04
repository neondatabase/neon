use pageserver_api::shard::TenantShardId;

use rand::seq::SliceRandom;
use tracing::{debug, info};
use utils::id::TenantTimelineId;

use tokio::task::JoinSet;

use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// Evict & on-demand download random layers.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    #[clap(long)]
    tasks_per_target: NonZeroUsize,
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

#[derive(Debug, Default)]
struct LiveStats {
    evictions: AtomicU64,
    downloads: AtomicU64,
}

impl LiveStats {
    fn eviction_done(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }
    fn download_done(&self) {
        self.downloads.fetch_add(1, Ordering::Relaxed);
    }
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

    let live_stats = Arc::new(LiveStats::default());
    tasks.spawn({
        let live_stats = Arc::clone(&live_stats);
        async move {
            let mut last_at = Instant::now();
            loop {
                tokio::time::sleep_until((last_at + Duration::from_secs(1)).into()).await;
                let now = Instant::now();
                let delta: Duration = now - last_at;
                last_at = now;

                let LiveStats {
                    evictions,
                    downloads,
                } = &*live_stats;
                let evictions = evictions.swap(0, Ordering::Relaxed) as f64 / delta.as_secs_f64();
                let downloads = downloads.swap(0, Ordering::Relaxed) as f64 / delta.as_secs_f64();
                info!("evictions={evictions:.2}/s downloads={downloads:.2}/s");
            }
        }
    });

    for tl in timelines {
        for _ in 0..args.tasks_per_target.get() {
            tasks.spawn(timeline_task(
                Arc::clone(&mgmt_api_client),
                tl,
                Arc::clone(&live_stats),
            ));
        }
    }

    while let Some(res) = tasks.join_next().await {
        res.unwrap();
    }
    Ok(())
}

async fn timeline_task(
    mgmt_api_client: Arc<pageserver_client::mgmt_api::Client>,
    timeline: TenantTimelineId,
    live_stats: Arc<LiveStats>,
) {
    // TODO: support sharding
    let tenant_shard_id = TenantShardId::unsharded(timeline.tenant_id);

    let mut layers = None;
    loop {
        if layers.is_none() {
            layers = Some(
                mgmt_api_client
                    .layer_map_info(tenant_shard_id, timeline.timeline_id)
                    .await
                    .unwrap(),
            );
        }

        let layer = {
            let mut rng = rand::thread_rng();
            layers
                .as_mut()
                .unwrap()
                .historic_layers
                .choose_mut(&mut rng)
                .expect("no layers")
        };
        #[derive(Clone, Copy)]
        enum Action {
            Evict,
            OnDemandDownload,
        }
        let action = if layer.is_remote() {
            Action::OnDemandDownload
        } else {
            Action::Evict
        };
        let did_it = match action {
            Action::Evict => {
                let did_it = mgmt_api_client
                    .layer_evict(
                        tenant_shard_id,
                        timeline.timeline_id,
                        layer.layer_file_name(),
                    )
                    .await
                    .unwrap();
                live_stats.eviction_done();
                did_it
            }
            Action::OnDemandDownload => {
                let did_it =mgmt_api_client
                    .layer_ondemand_download(
                        tenant_shard_id,
                        timeline.timeline_id,
                        layer.layer_file_name(),
                    )
                    .await
                    .unwrap();
                live_stats.download_done();
                did_it
            }
        };
        if !did_it {
            debug!("local copy of layer map appears out of sync, re-downloading");
            layers = None;
        } else {
            debug!("did it");
            layer.set_remote(match action {
                Action::Evict => true,
                Action::OnDemandDownload => false,
            });
        }
    }
}
