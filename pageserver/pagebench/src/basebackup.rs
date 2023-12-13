use anyhow::Context;
use pageserver_client::page_service::BasebackupRequest;
use utils::id::TenantId;
use utils::lsn::Lsn;

use rand::prelude::*;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{debug, info, instrument};
use utils::logging;

use std::cell::RefCell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::util::tenant_timeline_id::TenantTimelineId;

/// basebackup@LatestLSN
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "localhost:64000")]
    page_service_host_port: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long, default_value = "1.0")]
    gzip_probability: f64,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
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

struct Target {
    timeline: TenantTimelineId,
    lsn_range: Option<Range<Lsn>>,
}

async fn main_impl(
    args: Args,
    thread_local_stats: Arc<Mutex<Vec<Arc<Mutex<ThreadLocalStats>>>>>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    // discover targets
    let mut timelines: Vec<TenantTimelineId> = Vec::new();
    if args.targets.is_some() {
        timelines = args.targets.clone().unwrap();
    } else {
        let mut tenants: Vec<TenantId> = Vec::new();
        for ti in mgmt_api_client.list_tenants().await? {
            if !ti.id.is_unsharded() {
                anyhow::bail!(
                    "only unsharded tenants are supported at this time: {}",
                    ti.id
                );
            }
            tenants.push(ti.id.tenant_id)
        }
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
                    tenant_id,
                    timeline_id: tl.timeline_id,
                });
            }
        }
    }

    info!("timelines:\n{:?}", timelines);

    let mut js = JoinSet::new();
    for timeline in &timelines {
        js.spawn({
            let timeline = *timeline;
            let info = mgmt_api_client
                .timeline_info(timeline.tenant_id, timeline.timeline_id)
                .await
                .unwrap();
            async move {
                anyhow::Ok(Target {
                    timeline,
                    // TODO: lsn_range != latest LSN
                    lsn_range: Some(info.last_record_lsn..(info.last_record_lsn + 1)),
                })
            }
        });
    }
    let mut all_targets: Vec<Target> = Vec::new();
    while let Some(res) = js.join_next().await {
        all_targets.push(res.unwrap().unwrap());
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
        let (sender, receiver) = tokio::sync::mpsc::channel(1); // TODO: not sure what the implications of this are
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

    let work_sender = async move {
        start_work_barrier.wait().await;
        loop {
            let (timeline, work) = {
                let mut rng = rand::thread_rng();
                let target = all_targets.choose(&mut rng).unwrap();
                let lsn = target.lsn_range.clone().map(|r| rng.gen_range(r));
                (
                    target.timeline,
                    Work {
                        lsn,
                        gzip: rng.gen_bool(args.gzip_probability),
                    },
                )
            };
            let sender = work_senders.get(&timeline).unwrap();
            // TODO: what if this blocks?
            sender.send(work).await.ok().unwrap();
        }
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
                agg_stats.add(&stats);
            }
            agg_stats.output()
        },
    };

    let output = serde_json::to_string_pretty(&output).unwrap();
    println!("{output}");

    anyhow::Ok(())
}

#[derive(Copy, Clone)]
struct Work {
    lsn: Option<Lsn>,
    gzip: bool,
}

#[instrument(skip_all)]
async fn client(
    args: &'static Args,
    timeline: TenantTimelineId,
    start_work_barrier: Arc<Barrier>,
    mut work: tokio::sync::mpsc::Receiver<Work>,
    all_work_done_barrier: Arc<Barrier>,
    live_stats: Arc<LiveStats>,
) {
    start_work_barrier.wait().await;

    let client = pageserver_client::page_service::Client::new(crate::util::connstring::connstring(
        &args.page_service_host_port,
        args.pageserver_jwt.as_deref(),
    ))
    .await
    .unwrap();

    while let Some(Work { lsn, gzip }) = work.recv().await {
        let start = Instant::now();
        let copy_out_stream = client
            .basebackup(&BasebackupRequest {
                tenant_id: timeline.tenant_id,
                timeline_id: timeline.timeline_id,
                lsn,
                gzip,
            })
            .await
            .with_context(|| format!("start basebackup for {timeline}"))
            .unwrap();

        use futures::StreamExt;
        let size = Arc::new(AtomicUsize::new(0));
        copy_out_stream
            .for_each({
                |r| {
                    let size = Arc::clone(&size);
                    async move {
                        let size = Arc::clone(&size);
                        size.fetch_add(r.unwrap().len(), Ordering::Relaxed);
                    }
                }
            })
            .await;
        debug!("basebackup size is {} bytes", size.load(Ordering::Relaxed));
        let elapsed = start.elapsed();
        live_stats.inc();
        STATS.with(|stats| {
            stats.borrow().lock().unwrap().observe(elapsed).unwrap();
        });
    }

    all_work_done_barrier.wait().await;
}
