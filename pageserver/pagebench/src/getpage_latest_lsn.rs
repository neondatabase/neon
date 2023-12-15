use anyhow::Context;
use futures::future::join_all;
use pageserver::pgdatadir_mapping::key_to_rel_block;
use pageserver::repository;
use pageserver_api::key::is_rel_block_key;
use pageserver_api::shard::TenantShardId;
use pageserver_client::page_service::RelTagBlockNo;
use utils::lsn::Lsn;

use rand::prelude::*;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{info, instrument};
use utils::id::TenantShardId;
use utils::logging;

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::util::tenant_timeline_id::TenantTimelineId;

/// GetPage@LatestLSN, uniformly distributed across the compute-accessible keyspace.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    #[clap(long)]
    per_target_rate_limit: Option<usize>,
    targets: Option<Vec<TenantTimelineId>>,
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

#[derive(serde::Serialize)]
struct Output {
    total: PerTaskOutput,
}

const LATENCY_PERCENTILES: [f64; 4] = [95.0, 99.00, 99.90, 99.99];

struct LatencyPercentiles {
    latency_percentiles: [Duration; 4],
}

impl serde::Serialize for LatencyPercentiles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut ser = serializer.serialize_map(Some(LATENCY_PERCENTILES.len()))?;
        for p in LATENCY_PERCENTILES {
            ser.serialize_entry(
                &format!("p{p}"),
                &format!(
                    "{}",
                    &humantime::format_duration(self.latency_percentiles[0])
                ),
            )?;
        }
        ser.end()
    }
}

#[derive(serde::Serialize)]
struct PerTaskOutput {
    request_count: u64,
    #[serde(with = "humantime_serde")]
    latency_mean: Duration,
    latency_percentiles: LatencyPercentiles,
}

struct ThreadLocalStats {
    latency_histo: hdrhistogram::Histogram<u64>,
}

impl ThreadLocalStats {
    fn new() -> Self {
        Self {
            // Initialize with fixed bounds so that we panic at runtime instead of resizing the histogram,
            // which would skew the benchmark results.
            latency_histo: hdrhistogram::Histogram::new_with_bounds(1, 1_000_000_000, 3).unwrap(),
        }
    }
    fn observe(&mut self, latency: Duration) -> anyhow::Result<()> {
        let micros: u64 = latency
            .as_micros()
            .try_into()
            .context("latency greater than u64")?;
        self.latency_histo
            .record(micros)
            .context("add to histogram")?;
        Ok(())
    }
    fn output(&self) -> PerTaskOutput {
        let latency_percentiles = std::array::from_fn(|idx| {
            let micros = self
                .latency_histo
                .value_at_percentile(LATENCY_PERCENTILES[idx]);
            Duration::from_micros(micros)
        });
        PerTaskOutput {
            request_count: self.latency_histo.len(),
            latency_mean: Duration::from_micros(self.latency_histo.mean() as u64),
            latency_percentiles: LatencyPercentiles {
                latency_percentiles,
            },
        }
    }

    fn add(&mut self, other: &Self) {
        let Self {
            ref mut latency_histo,
        } = self;
        latency_histo.add(&other.latency_histo).unwrap();
    }
}

thread_local! {
    pub static STATS: RefCell<Arc<Mutex<ThreadLocalStats>>> = std::cell::RefCell::new(
        Arc::new(Mutex::new(ThreadLocalStats::new()))
    );
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let _guard = logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();

    let thread_local_stats = Arc::new(Mutex::new(Vec::new()));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .on_thread_start({
            let thread_local_stats = Arc::clone(&thread_local_stats);
            move || {
                // pre-initialize the histograms
                STATS.with(|stats| {
                    let stats: Arc<_> = Arc::clone(&*stats.borrow());
                    thread_local_stats.lock().unwrap().push(stats);
                });
            }
        })
        .enable_all()
        .build()
        .unwrap();

    let main_task = rt.spawn(main_impl(args, thread_local_stats));
    rt.block_on(main_task).unwrap()
}

#[derive(Clone)]
struct KeyRange {
    timeline: TenantTimelineId,
    timeline_lsn: Lsn,
    start: i128,
    end: i128,
}

impl KeyRange {
    fn len(&self) -> i128 {
        self.end - self.start
    }
}

async fn main_impl(
    args: Args,
    thread_local_stats: Arc<Mutex<Vec<Arc<Mutex<ThreadLocalStats>>>>>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        None, // TODO: support jwt in args
    ));

    // discover targets
    let mut timelines: Vec<TenantTimelineId> = Vec::new();
    if args.targets.is_some() {
        timelines = args.targets.clone().unwrap();
    } else {
        let tenants: Vec<TenantShardId> = mgmt_api_client
            .list_tenants()
            .await?
            .into_iter()
            .map(|ti| ti.id)
            .collect();
        let mut js = JoinSet::new();
        for tenant_id in tenants {
            js.spawn({
                let mgmt_api_client = Arc::clone(&mgmt_api_client);
                async move {
                    (
                        tenant_id,
                        mgmt_api_client.list_timelines(tenant_id).await.unwrap(),
                    )
                }
            });
        }
        while let Some(res) = js.join_next().await {
            let (tenant_id, tl_infos) = res.unwrap();
            for tl in tl_infos {
                timelines.push(TenantTimelineId {
                    tenant_shard_id: tenant_id,
                    timeline_id: tl.timeline_id,
                });
            }
        }
    }

    info!("timelines:\n{:?}", timelines);

    let mut js = JoinSet::new();
    for timeline in &timelines {
        js.spawn({
            let mgmt_api_client = Arc::clone(&mgmt_api_client);
            let timeline = *timeline;
            async move {
                let partitioning = mgmt_api_client
                    .keyspace(timeline.tenant_shard_id, timeline.timeline_id)
                    .await?;
                let lsn = partitioning.at_lsn;

                let ranges = partitioning
                    .keys
                    .ranges
                    .iter()
                    .filter_map(|r| {
                        let start = r.start;
                        let end = r.end;
                        // filter out non-relblock keys
                        match (is_rel_block_key(&start), is_rel_block_key(&end)) {
                            (true, true) => Some(KeyRange {
                                timeline,
                                timeline_lsn: lsn,
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

                anyhow::Ok(ranges)
            }
        });
    }
    let mut all_ranges: Vec<KeyRange> = Vec::new();
    while let Some(res) = js.join_next().await {
        all_ranges.extend(res.unwrap().unwrap());
    }

    let live_stats = Arc::new(LiveStats::default());

    let num_client_tasks = timelines.len();
    let num_live_stats_dump = 1;
    let num_work_sender_tasks = 1;

    let start_work_barrier = Arc::new(tokio::sync::Barrier::new(
        num_client_tasks + num_live_stats_dump + num_work_sender_tasks,
    ));
    let all_work_done_barrier = Arc::new(tokio::sync::Barrier::new(num_client_tasks));

    tokio::spawn({
        let stats = Arc::clone(&live_stats);
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

    let mut work_senders = HashMap::new();
    let mut tasks = Vec::new();
    for tl in &timelines {
        let (sender, receiver) = tokio::sync::mpsc::channel(10); // TODO: not sure what the implications of this are
        work_senders.insert(tl, sender);
        tasks.push(tokio::spawn(client(
            args,
            *tl,
            Arc::clone(&start_work_barrier),
            receiver,
            Arc::clone(&all_work_done_barrier),
            Arc::clone(&live_stats),
        )));
    }

    let work_sender: Pin<Box<dyn Send + Future<Output = ()>>> = match args.per_target_rate_limit {
        None => Box::pin(async move {
            let weights = rand::distributions::weighted::WeightedIndex::new(
                all_ranges.iter().map(|v| v.len()),
            )
            .unwrap();

            start_work_barrier.wait().await;

            loop {
                let (range, key) = {
                    let mut rng = rand::thread_rng();
                    let r = &all_ranges[weights.sample(&mut rng)];
                    let key: i128 = rng.gen_range(r.start..r.end);
                    let key = repository::Key::from_i128(key);
                    let (rel_tag, block_no) =
                        key_to_rel_block(key).expect("we filter non-rel-block keys out above");
                    (r, RelTagBlockNo { rel_tag, block_no })
                };
                let sender = work_senders.get(&range.timeline).unwrap();
                // TODO: what if this blocks?
                sender.send((key, range.timeline_lsn)).await.ok().unwrap();
            }
        }),
        Some(rps_limit) => Box::pin(async move {
            let period = Duration::from_secs_f64(1.0 / (rps_limit as f64));

            let make_timeline_task: &dyn Fn(
                TenantTimelineId,
            )
                -> Pin<Box<dyn Send + Future<Output = ()>>> = &|timeline| {
                let sender = work_senders.get(&timeline).unwrap();
                let ranges: Vec<KeyRange> = all_ranges
                    .iter()
                    .filter(|r| r.timeline == timeline)
                    .cloned()
                    .collect();
                let weights = rand::distributions::weighted::WeightedIndex::new(
                    ranges.iter().map(|v| v.len()),
                )
                .unwrap();

                Box::pin(async move {
                    let mut ticker = tokio::time::interval(period);
                    ticker.set_missed_tick_behavior(
                        /* TODO review this choice */
                        tokio::time::MissedTickBehavior::Burst,
                    );
                    loop {
                        ticker.tick().await;
                        let (range, key) = {
                            let mut rng = rand::thread_rng();
                            let r = &ranges[weights.sample(&mut rng)];
                            let key: i128 = rng.gen_range(r.start..r.end);
                            let key = repository::Key::from_i128(key);
                            let (rel_tag, block_no) = key_to_rel_block(key)
                                .expect("we filter non-rel-block keys out above");
                            (r, RelTagBlockNo { rel_tag, block_no })
                        };
                        sender.send((key, range.timeline_lsn)).await.ok().unwrap();
                    }
                })
            };

            let tasks: Vec<_> = work_senders
                .keys()
                .map(|tl| make_timeline_task(**tl))
                .collect();

            start_work_barrier.wait().await;

            join_all(tasks).await;
        }),
    };

    if let Some(runtime) = args.runtime {
        match tokio::time::timeout(runtime.into(), work_sender).await {
            Ok(()) => unreachable!("work sender never terminates"),
            Err(_timeout) => {
                // this implicitly drops the work_senders, making all the clients exit
            }
        }
    } else {
        work_sender.await;
        unreachable!("work sender never terminates");
    }

    for t in tasks {
        t.await.unwrap();
    }

    let output = Output {
        total: {
            let mut agg_stats = ThreadLocalStats::new();
            for stats in thread_local_stats.lock().unwrap().iter() {
                let stats = stats.lock().unwrap();
                agg_stats.add(&*stats);
            }
            agg_stats.output()
        },
    };

    let output = serde_json::to_string_pretty(&output).unwrap();
    println!("{output}");

    anyhow::Ok(())
}

#[instrument(skip_all)]
async fn client(
    args: &'static Args,
    timeline: TenantTimelineId,
    start_work_barrier: Arc<Barrier>,
    mut work: tokio::sync::mpsc::Receiver<(RelTagBlockNo, Lsn)>,
    all_work_done_barrier: Arc<Barrier>,
    live_stats: Arc<LiveStats>,
) {
    start_work_barrier.wait().await;

    let client = pageserver_client::page_service::Client::new(args.page_service_connstring.clone())
        .await
        .unwrap();
    let mut client = client
        .pagestream(timeline.tenant_shard_id, timeline.timeline_id)
        .await
        .unwrap();

    while let Some((key, lsn)) = work.recv().await {
        let start = Instant::now();
        client
            .getpage(key, lsn)
            .await
            .with_context(|| format!("getpage for {timeline}"))
            .unwrap();
        let elapsed = start.elapsed();
        live_stats.inc();
        STATS.with(|stats| {
            stats.borrow().lock().unwrap().observe(elapsed).unwrap();
        });
    }

    all_work_done_barrier.wait().await;
}
