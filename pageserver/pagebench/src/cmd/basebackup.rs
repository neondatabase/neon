use anyhow::Context;
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api::ForceAwaitLogicalSize;
use pageserver_client::page_service::BasebackupRequest;

use utils::id::TenantTimelineId;
use utils::lsn::Lsn;

use rand::prelude::*;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{info, instrument};

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::util::tokio_thread_local_stats::AllThreadLocalStats;
use crate::util::{request_stats, tokio_thread_local_stats};

/// basebackup@LatestLSN
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long, default_value = "1.0")]
    gzip_probability: f64,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
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

struct Target {
    timeline: TenantTimelineId,
    lsn_range: Option<Range<Lsn>>,
}

#[derive(serde::Serialize)]
struct Output {
    total: request_stats::Output,
}

tokio_thread_local_stats::declare!(STATS: request_stats::Stats);

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    tokio_thread_local_stats::main!(STATS, move |thread_local_stats| {
        main_impl(args, thread_local_stats)
    })
}

async fn main_impl(
    args: Args,
    all_thread_local_stats: AllThreadLocalStats<request_stats::Stats>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: args.limit_to_first_n_targets,
            targets: args.targets.clone(),
        },
    )
    .await?;
    let mut js = JoinSet::new();
    for timeline in &timelines {
        js.spawn({
            let timeline = *timeline;
            let info = mgmt_api_client
                .timeline_info(
                    TenantShardId::unsharded(timeline.tenant_id),
                    timeline.timeline_id,
                    ForceAwaitLogicalSize::No,
                )
                .await
                .unwrap();
            async move {
                anyhow::Ok(Target {
                    timeline,
                    // TODO: support lsn_range != latest LSN
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
            let mut agg_stats = request_stats::Stats::new();
            for stats in all_thread_local_stats.lock().unwrap().iter() {
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

    let client = pageserver_client::page_service::Client::new(args.page_service_connstring.clone())
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
        info!("basebackup size is {} bytes", size.load(Ordering::Relaxed));
        let elapsed = start.elapsed();
        live_stats.inc();
        STATS.with(|stats| {
            stats.borrow().lock().unwrap().observe(elapsed).unwrap();
        });
    }

    all_work_done_barrier.wait().await;
}
