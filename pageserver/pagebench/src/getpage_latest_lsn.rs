mod tenant_timeline_id;

use anyhow::Context;
use pageserver::client::mgmt_api::Client;
use pageserver::client::page_service::RelTagBlockNo;
use pageserver::pgdatadir_mapping::{is_rel_block_key, key_to_rel_block};
use pageserver::{repository, tenant};
use std::sync::Weak;
use tokio_util::sync::CancellationToken;
use utils::lsn::Lsn;

use pageserver::tenant::Tenant;
use rand::prelude::*;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{info, instrument};
use utils::id::{TenantId, TimelineId};
use utils::logging;

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use self::tenant_timeline_id::TenantTimelineId;

/// GetPage@LatestLSN, uniformly distributed across the compute-accessible keyspace.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    // targets: Option<Vec<TenantTimelineId>>,
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
    logging::init(
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

struct ClientPool {
    cache: HashMap<TenantTimelineId, Vec<Weak<Mutex<pageserver::client::page_service::Client>>>>,
    lru: VecDeque<Arc<Mutex<pageserver::client::page_service::Client>>>,
}

impl ClientPool {
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
            lru: Default::default(),
        }
    }

    pub fn take(
        &mut self,
        timeline: TenantTimelineId,
    ) -> Option<Arc<Mutex<pageserver::client::page_service::Client>>> {
        match self.cache.entry(timeline) {
            Entry::Occupied(mut o) => {
                while let Some(weak) = o.get_mut().pop() {
                    if let Some(strong) = Weak::upgrade(&weak) {
                        return Some(strong);
                    }
                }
                None
            }
            Entry::Vacant(_) => None,
        }
    }

    pub fn put(
        &mut self,
        timeline: TenantTimelineId,
        client: Arc<Mutex<pageserver::client::page_service::Client>>,
    ) {
        match self.cache.entry(timeline) {
            Entry::Occupied(mut o) => o.get_mut().push(Arc::downgrade(&client)),
            Entry::Vacant(v) => todo!(),
        }
        self.lru.push_front(client);
        self.lru.truncate(1000);
    }
}

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

struct Targets {
    ranges: Vec<KeyRange>,
    weights: Vec<usize>,
}

async fn main_impl(
    args: Args,
    thread_local_stats: Arc<Mutex<Vec<Arc<Mutex<ThreadLocalStats>>>>>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver::client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
    ));

    // discover targets
    let mut targets: Vec<TenantTimelineId> = Vec::new();
    if false {
        targets = targets.clone();
    } else {
        let tenants: Vec<TenantId> = mgmt_api_client
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
            let (tenant_id, timelines) = res.unwrap();
            for tl in timelines {
                targets.push(TenantTimelineId {
                    tenant_id,
                    timeline_id: tl.timeline_id,
                });
            }
        }
    }

    info!("targets:\n{:?}", targets);

    let mut js = JoinSet::new();
    for target in targets {
        js.spawn({
            let mgmt_api_client = Arc::clone(&mgmt_api_client);
            async move {
                let partitioning = mgmt_api_client
                    .keyspace(target.tenant_id, target.timeline_id)
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
                        match (is_rel_block_key(start), is_rel_block_key(end)) {
                            (true, true) => Some(KeyRange {
                                timeline: target,
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
    while let Some(ranges) = js.join_next().await {
        all_ranges.extend(ranges.unwrap().unwrap());
    }
    let mut all_weights: Vec<usize> = all_ranges
        .iter()
        .map(|v| (v.end - v.start).try_into().unwrap())
        .collect();
    let targets = Arc::new(Targets {
        ranges: all_ranges,
        weights: all_weights,
    });

    let stats = Arc::new(LiveStats::default());

    let num_work_tasks = args.num_clients.get();
    let num_live_stats_dump = 1;

    let start_work_barrier = Arc::new(tokio::sync::Barrier::new(
        num_work_tasks + num_live_stats_dump,
    ));
    let all_work_done_barrier = Arc::new(tokio::sync::Barrier::new(num_work_tasks));

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

    let pool = Arc::new(Mutex::new(ClientPool::new()));

    let cancel = CancellationToken::new();

    let mut tasks = Vec::new();
    for client_id in 0..args.num_clients.get() {
        let live_stats = Arc::clone(&stats);
        let t = tokio::spawn(client(
            args,
            client_id,
            Arc::clone(&mgmt_api_client),
            Arc::clone(&pool),
            Arc::clone(&start_work_barrier),
            Arc::clone(&all_work_done_barrier),
            Arc::clone(&targets),
            live_stats,
            cancel.clone(),
        ));
        tasks.push(t);
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

#[instrument(skip_all, %client_id)]
async fn client(
    args: &'static Args,
    client_id: usize,
    mgmt_api_client: Arc<pageserver::client::mgmt_api::Client>,
    pool: Arc<Mutex<ClientPool>>,
    start_work_barrier: Arc<Barrier>,
    all_work_done_barrier: Arc<Barrier>,
    targets: Arc<Targets>,
    live_stats: Arc<LiveStats>,
    cancel: CancellationToken,
) {
    start_work_barrier.wait().await;

    while !cancel.is_cancelled() {
        let (range, key) = {
            let mut rng = rand::thread_rng();
            let r = targets
                .ranges
                .choose_weighted(&mut rng, |range| range.len())
                .unwrap();
            let key: i128 = rng.gen_range(r.start..r.end);
            let key = repository::Key::from_i128(key);
            let (rel_tag, block_no) =
                key_to_rel_block(key).expect("we filter non-rel-block keys out above");
            (r, RelTagBlockNo { rel_tag, block_no })
        };

        let client = match pool.lock().unwrap().take(range.timeline) {
            Some(client) => client,
            None => Arc::new(Mutex::new(
                pageserver::client::page_service::Client::new(
                    args.page_service_connstring.clone(),
                    range.timeline.tenant_id,
                    range.timeline.timeline_id,
                )
                .await
                .unwrap(),
            )),
        };

        let start = Instant::now();
        client
            .lock()
            .unwrap()
            .getpage(key, range.timeline_lsn)
            .await
            .with_context(|| format!("getpage for {}", range.timeline))
            .unwrap();
        let elapsed = start.elapsed();
        live_stats.inc();
        STATS.with(|stats| {
            stats.borrow().lock().unwrap().observe(elapsed).unwrap();
        });

        pool.lock().unwrap().put(range.timeline, client);
    }

    all_work_done_barrier.wait().await;
}

// async fn timeline(
//     args: &'static Args,
//     mgmt_api_client: Arc<pageserver::client::mgmt_api::Client>,
//     tenant_id: TenantId,
//     timeline_id: TimelineId,
//     start_work_barrier: Arc<Barrier>,
//     all_work_done_barrier: Arc<Barrier>,
//     live_stats: Arc<LiveStats>,
// ) -> anyhow::Result<()> {
//     let mut tasks = Vec::new();

//     for _i in 0..args.num_tasks {
//         let ranges = ranges.clone();
//         let _weights = weights.clone();
//         let start_work_barrier = Arc::clone(&start_work_barrier);
//         let all_work_done_barrier = Arc::clone(&all_work_done_barrier);

//         let jh = tokio::spawn({
//             let live_stats = Arc::clone(&live_stats);
//             async move {
//                 let mut getpage_client = pageserver::client::page_service::Client::new(
//                     args.page_service_connstring.clone(),
//                     tenant_id,
//                     timeline_id,
//                 )
//                 .await
//                 .unwrap();

//                 start_work_barrier.wait().await;
//                 for _i in 0..args.num_requests {
//                     let key = {
//                         let mut rng = rand::thread_rng();
//                         let r = ranges.choose_weighted(&mut rng, |r| r.len()).unwrap();
//                         let key: i128 = rng.gen_range(r.start..r.end);
//                         let key = repository::Key::from_i128(key);
//                         let (rel_tag, block_no) =
//                             key_to_rel_block(key).expect("we filter non-rel-block keys out above");
//                         RelTagBlockNo { rel_tag, block_no }
//                     };
//                     let start = Instant::now();
//                     getpage_client
//                         .getpage(key, lsn)
//                         .await
//                         .with_context(|| {
//                             format!("getpage for tenant {} timeline {}", tenant_id, timeline_id)
//                         })
//                         .unwrap();
//                     let elapsed = start.elapsed();
//                     live_stats.inc();
//                     STATS.with(|stats| {
//                         stats.borrow().lock().unwrap().observe(elapsed).unwrap();
//                     });
//                 }
//                 all_work_done_barrier.wait().await;

//                 getpage_client.shutdown().await;
//             }
//         });
//         tasks.push(jh);
//     }

//     for task in tasks {
//         task.await.unwrap();
//     }

//     Ok(())
// }
