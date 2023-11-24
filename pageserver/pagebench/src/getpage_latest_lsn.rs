use anyhow::Context;
use pageserver::client::page_service::RelTagBlockNo;
use pageserver::pgdatadir_mapping::{is_rel_block_key, key_to_rel_block};
use pageserver::repository;

use rand::prelude::*;
use tokio::sync::Barrier;
use tracing::info;
use utils::id::{TenantId, TimelineId};
use utils::logging;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::task::JoinHandle;

/// Measure performance of the GetPage API, targeting the latest LSN.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    num_tasks: usize,
    #[clap(long)]
    num_requests: usize,
    #[clap(long)]
    pick_n_tenants: Option<usize>,
    tenants: Option<Vec<TenantId>>,
}

#[derive(Debug, Default)]
struct LiveStats {
    completed_requests: AtomicU64,
}

impl LiveStats {
    fn inc(&self) {
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) async fn main(args: Args) -> anyhow::Result<()> {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();

    let args: &'static Args = Box::leak(Box::new(args));

    let client = Arc::new(pageserver::client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
    ));

    let mut tenants: Vec<TenantId> = if let Some(tenants) = &args.tenants {
        tenants.clone()
    } else {
        client
            .list_tenants()
            .await?
            .into_iter()
            .map(|ti| ti.id)
            .collect()
    };
    let tenants = if let Some(n) = args.pick_n_tenants {
        tenants.truncate(n);
        if tenants.len() != n {
            anyhow::bail!("too few tenants: {} < {}", tenants.len(), n);
        }
        tenants
    } else {
        tenants
    };

    let mut tenant_timelines = Vec::new();
    for tenant_id in tenants {
        tenant_timelines.extend(
            client
                .list_timelines(tenant_id)
                .await?
                .into_iter()
                .map(|ti| (tenant_id, ti.timeline_id)),
        );
    }
    info!("tenant_timelines:\n{:?}", tenant_timelines);

    let stats = Arc::new(LiveStats::default());

    let num_work_tasks = tenant_timelines.len() * args.num_tasks;

    let start_work_barrier = Arc::new(tokio::sync::Barrier::new(num_work_tasks + 1));

    tokio::spawn({
        let stats = Arc::clone(&stats);
        let start_work_barrier = Arc::clone(&start_work_barrier);
        async move {
            start_work_barrier.wait().await;
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                let elapsed = start.elapsed();
                info!(
                    "RPS: {:.0}",
                    completed_requests as f64 / elapsed.as_secs_f64()
                );
            }
        }
    });

    let mut tasks = Vec::new();
    for (tenant_id, timeline_id) in tenant_timelines {
        let stats = Arc::clone(&stats);
        let t = tokio::spawn(timeline(
            args,
            client.clone(),
            tenant_id,
            timeline_id,
            Arc::clone(&start_work_barrier),
            stats,
        ));
        tasks.push(t);
    }

    for t in tasks {
        t.await.unwrap().unwrap();
    }

    anyhow::Ok(())
}

async fn timeline(
    args: &'static Args,
    mgmt_api_client: Arc<pageserver::client::mgmt_api::Client>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    start_work_barrier: Arc<Barrier>,
    stats: Arc<LiveStats>,
) -> anyhow::Result<()> {
    let partitioning = mgmt_api_client.keyspace(tenant_id, timeline_id).await?;
    let lsn = partitioning.at_lsn;

    struct KeyRange {
        start: i128,
        end: i128,
    }

    impl KeyRange {
        fn len(&self) -> i128 {
            self.end - self.start
        }
    }

    let ranges = partitioning
        .keys
        .ranges
        .iter()
        .filter_map(|r| {
            let start = r.start;
            let end = r.end;
            // filter out non-relblock keys
            match (is_rel_block_key(start), is_rel_block_key(end)) {
                (true, true) => Some(KeyRange {
                    start: start.to_i128(),
                    end: end.to_i128(),
                }),
                (true, false) | (false, true) => {
                    unimplemented!("split up range")
                }
                (false, false) => None,
            }
        })
        .collect::<Vec<_>>();

    // weighted ranges
    let weights = ranges.iter().map(|r| r.len()).collect::<Vec<_>>();

    let ranges = Arc::new(ranges);
    let weights = Arc::new(weights);

    let mut tasks = Vec::<JoinHandle<()>>::new();

    for _i in 0..args.num_tasks {
        let ranges = ranges.clone();
        let _weights = weights.clone();
        let start_work_barrier = Arc::clone(&start_work_barrier);
        let task = tokio::spawn({
            let stats = Arc::clone(&stats);
            async move {
                let mut getpage_client = pageserver::client::page_service::Client::new(
                    args.page_service_connstring.clone(),
                    tenant_id,
                    timeline_id,
                )
                .await
                .unwrap();
                start_work_barrier.wait().await;
                for _i in 0..args.num_requests {
                    let key = {
                        let mut rng = rand::thread_rng();
                        let r = ranges.choose_weighted(&mut rng, |r| r.len()).unwrap();
                        let key: i128 = rng.gen_range(r.start..r.end);
                        let key = repository::Key::from_i128(key);
                        let (rel_tag, block_no) =
                            key_to_rel_block(key).expect("we filter non-rel-block keys out above");
                        RelTagBlockNo { rel_tag, block_no }
                    };
                    getpage_client
                        .getpage(key, lsn)
                        .await
                        .with_context(|| {
                            format!("getpage for tenant {} timeline {}", tenant_id, timeline_id)
                        })
                        .unwrap();
                    stats.inc();
                }
                getpage_client.shutdown().await;
            }
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    Ok(())
}
