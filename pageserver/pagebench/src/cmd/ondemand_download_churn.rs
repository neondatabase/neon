use pageserver_api::{models::HistoricLayerInfo, shard::TenantShardId};

use pageserver_client::mgmt_api;
use rand::seq::SliceRandom;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use utils::id::{TenantTimelineId, TimelineId};

use std::{f64, sync::Arc};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit},
    task::JoinSet,
};

use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering},
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
    #[clap(long, default_value = "1")]
    tasks_per_target: NonZeroUsize,
    #[clap(long, default_value = "1")]
    concurrency_per_target: NonZeroUsize,
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

#[derive(serde::Serialize)]
struct Output {
    downloads_count: u64,
    downloads_bytes: u64,
    evictions_count: u64,
    timeline_restarts: u64,
    #[serde(with = "humantime_serde")]
    runtime: Duration,
}

#[derive(Debug, Default)]
struct LiveStats {
    evictions_count: AtomicU64,
    downloads_count: AtomicU64,
    downloads_bytes: AtomicU64,
    timeline_restarts: AtomicU64,
}

impl LiveStats {
    fn eviction_done(&self) {
        self.evictions_count.fetch_add(1, Ordering::Relaxed);
    }
    fn download_done(&self, size: u64) {
        self.downloads_count.fetch_add(1, Ordering::Relaxed);
        self.downloads_bytes.fetch_add(size, Ordering::Relaxed);
    }
    fn timeline_restart_done(&self) {
        self.timeline_restarts.fetch_add(1, Ordering::Relaxed);
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

    let token = CancellationToken::new();
    let mut tasks = JoinSet::new();

    let periodic_stats = Arc::new(LiveStats::default());
    let total_stats = Arc::new(LiveStats::default());

    let start = Instant::now();
    tasks.spawn({
        let periodic_stats = Arc::clone(&periodic_stats);
        let total_stats = Arc::clone(&total_stats);
        let cloned_token = token.clone();
        async move {
            let mut last_at = Instant::now();
            loop {
                if cloned_token.is_cancelled() {
                    return;
                }
                tokio::time::sleep_until((last_at + Duration::from_secs(1)).into()).await;
                let now = Instant::now();
                let delta: Duration = now - last_at;
                last_at = now;

                let LiveStats {
                    evictions_count,
                    downloads_count,
                    downloads_bytes,
                    timeline_restarts,
                } = &*periodic_stats;
                let evictions_count = evictions_count.swap(0, Ordering::Relaxed);
                let downloads_count = downloads_count.swap(0, Ordering::Relaxed);
                let downloads_bytes = downloads_bytes.swap(0, Ordering::Relaxed);
                let timeline_restarts = timeline_restarts.swap(0, Ordering::Relaxed);

                total_stats.evictions_count.fetch_add(evictions_count, Ordering::Relaxed);
                total_stats.downloads_count.fetch_add(downloads_count, Ordering::Relaxed);
                total_stats.downloads_bytes.fetch_add(downloads_bytes, Ordering::Relaxed);
                total_stats.timeline_restarts.fetch_add(timeline_restarts, Ordering::Relaxed);

                let evictions_per_s = evictions_count as f64 / delta.as_secs_f64();
                let downloads_per_s = downloads_count as f64 / delta.as_secs_f64();
                let downloads_mibs_per_s = downloads_bytes as f64 / delta.as_secs_f64() / ((1 << 20) as f64);

                info!("evictions={evictions_per_s:.2}/s downloads={downloads_per_s:.2}/s download_bytes={downloads_mibs_per_s:.2}MiB/s timeline_restarts={timeline_restarts}");
            }
        }
    });

    for tl in timelines {
        for _ in 0..args.tasks_per_target.get() {
            tasks.spawn(timeline_actor(
                args,
                Arc::clone(&mgmt_api_client),
                tl,
                Arc::clone(&periodic_stats),
                token.clone(),
            ));
        }
    }
    if let Some(runtime) = args.runtime {
        tokio::spawn(async move {
            tokio::time::sleep(runtime.into()).await;
            token.cancel();
        });
    }

    while let Some(res) = tasks.join_next().await {
        res.unwrap();
    }
    let end = Instant::now();
    let duration: Duration = end - start;

    let output = {
        let LiveStats {
            evictions_count,
            downloads_count,
            downloads_bytes,
            timeline_restarts,
        } = &*total_stats;
        Output {
            downloads_count: downloads_count.load(Ordering::Relaxed),
            downloads_bytes: downloads_bytes.load(Ordering::Relaxed),
            evictions_count: evictions_count.load(Ordering::Relaxed),
            timeline_restarts: timeline_restarts.load(Ordering::Relaxed),
            runtime: duration,
        }
    };
    let output = serde_json::to_string_pretty(&output).unwrap();
    println!("{output}");

    Ok(())
}

async fn timeline_actor(
    args: &'static Args,
    mgmt_api_client: Arc<pageserver_client::mgmt_api::Client>,
    timeline: TenantTimelineId,
    live_stats: Arc<LiveStats>,
    token: CancellationToken,
) {
    // TODO: support sharding
    let tenant_shard_id = TenantShardId::unsharded(timeline.tenant_id);

    struct Timeline {
        joinset: JoinSet<()>,
        layers: Vec<mpsc::Sender<OwnedSemaphorePermit>>,
        concurrency: Arc<tokio::sync::Semaphore>,
    }
    while !token.is_cancelled() {
        debug!("restarting timeline");
        let layer_map_info = mgmt_api_client
            .layer_map_info(tenant_shard_id, timeline.timeline_id)
            .await
            .unwrap();
        let concurrency = Arc::new(tokio::sync::Semaphore::new(
            args.concurrency_per_target.get(),
        ));

        let mut joinset = JoinSet::new();
        let layers = layer_map_info
            .historic_layers
            .into_iter()
            .map(|historic_layer| {
                let (tx, rx) = mpsc::channel(1);
                joinset.spawn(layer_actor(
                    tenant_shard_id,
                    timeline.timeline_id,
                    historic_layer,
                    rx,
                    Arc::clone(&mgmt_api_client),
                    Arc::clone(&live_stats),
                ));
                tx
            })
            .collect::<Vec<_>>();

        let mut timeline = Timeline {
            joinset,
            layers,
            concurrency,
        };

        live_stats.timeline_restart_done();

        while !token.is_cancelled() {
            assert!(!timeline.joinset.is_empty());
            if let Some(res) = timeline.joinset.try_join_next() {
                debug!(?res, "a layer actor exited, should not happen");
                timeline.joinset.shutdown().await;
                break;
            }

            let mut permit = Some(
                Arc::clone(&timeline.concurrency)
                    .acquire_owned()
                    .await
                    .unwrap(),
            );

            loop {
                let layer_tx = {
                    let mut rng = rand::thread_rng();
                    timeline.layers.choose_mut(&mut rng).expect("no layers")
                };
                match layer_tx.try_send(permit.take().unwrap()) {
                    Ok(_) => break,
                    Err(e) => match e {
                        mpsc::error::TrySendError::Full(back) => {
                            // TODO: retrying introduces bias away from slow downloaders
                            permit.replace(back);
                        }
                        mpsc::error::TrySendError::Closed(_) => panic!(),
                    },
                }
            }
        }
    }
}

async fn layer_actor(
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    mut layer: HistoricLayerInfo,
    mut rx: mpsc::Receiver<tokio::sync::OwnedSemaphorePermit>,
    mgmt_api_client: Arc<mgmt_api::Client>,
    live_stats: Arc<LiveStats>,
) {
    #[derive(Clone, Copy)]
    enum Action {
        Evict,
        OnDemandDownload,
    }

    while let Some(_permit) = rx.recv().await {
        let action = if layer.is_remote() {
            Action::OnDemandDownload
        } else {
            Action::Evict
        };

        let did_it = match action {
            Action::Evict => {
                let did_it = mgmt_api_client
                    .layer_evict(tenant_shard_id, timeline_id, layer.layer_file_name())
                    .await
                    .unwrap();
                live_stats.eviction_done();
                did_it
            }
            Action::OnDemandDownload => {
                let did_it = mgmt_api_client
                    .layer_ondemand_download(tenant_shard_id, timeline_id, layer.layer_file_name())
                    .await
                    .unwrap();
                live_stats.download_done(layer.layer_file_size());
                did_it
            }
        };
        if !did_it {
            debug!("local copy of layer map appears out of sync, re-downloading");
            return;
        }
        debug!("did it");
        layer.set_remote(match action {
            Action::Evict => true,
            Action::OnDemandDownload => false,
        });
    }
}
