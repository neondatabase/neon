use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::anyhow;
use futures::TryStreamExt as _;
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api::ForceAwaitLogicalSize;
use pageserver_client::page_service::BasebackupRequest;
use pageserver_page_api as page_api;
use rand::prelude::*;
use tokio::io::AsyncRead;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio_util::compat::{TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _};
use tokio_util::io::StreamReader;
use tonic::async_trait;
use tracing::{info, instrument};
use url::Url;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;
use utils::shard::ShardIndex;

use crate::util::tokio_thread_local_stats::AllThreadLocalStats;
use crate::util::{request_stats, tokio_thread_local_stats};

/// basebackup@LatestLSN
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    /// The Pageserver to connect to. Use postgresql:// for libpq, or grpc:// for gRPC.
    #[clap(long, default_value = "postgresql://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long)]
    no_compression: bool,
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
        reqwest::Client::new(), // TODO: support ssl_ca_file for https APIs in pagebench.
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
    let scheme = match Url::parse(&args.page_service_connstring) {
        Ok(url) => url.scheme().to_lowercase().to_string(),
        Err(url::ParseError::RelativeUrlWithoutBase) => "postgresql".to_string(),
        Err(err) => return Err(anyhow!("invalid connstring: {err}")),
    };
    for &tl in &timelines {
        let (sender, receiver) = tokio::sync::mpsc::channel(1); // TODO: not sure what the implications of this are
        work_senders.insert(tl, sender);

        let client: Box<dyn Client> = match scheme.as_str() {
            "postgresql" | "postgres" => Box::new(
                LibpqClient::new(&args.page_service_connstring, tl, !args.no_compression).await?,
            ),
            "grpc" => Box::new(
                GrpcClient::new(&args.page_service_connstring, tl, !args.no_compression).await?,
            ),
            scheme => return Err(anyhow!("invalid scheme {scheme}")),
        };

        tasks.push(tokio::spawn(run_worker(
            client,
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
                (target.timeline, Work { lsn })
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
}

#[instrument(skip_all)]
async fn run_worker(
    mut client: Box<dyn Client>,
    start_work_barrier: Arc<Barrier>,
    mut work: tokio::sync::mpsc::Receiver<Work>,
    all_work_done_barrier: Arc<Barrier>,
    live_stats: Arc<LiveStats>,
) {
    start_work_barrier.wait().await;

    while let Some(Work { lsn }) = work.recv().await {
        let start = Instant::now();
        let stream = client.basebackup(lsn).await.unwrap();

        let size = futures::io::copy(stream.compat(), &mut tokio::io::sink().compat_write())
            .await
            .unwrap();
        info!("basebackup size is {size} bytes");
        let elapsed = start.elapsed();
        live_stats.inc();
        STATS.with(|stats| {
            stats.borrow().lock().unwrap().observe(elapsed).unwrap();
        });
    }

    all_work_done_barrier.wait().await;
}

/// A basebackup client. This allows switching out the client protocol implementation.
#[async_trait]
trait Client: Send {
    async fn basebackup(
        &mut self,
        lsn: Option<Lsn>,
    ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send>>>;
}

/// A libpq-based Pageserver client.
struct LibpqClient {
    inner: pageserver_client::page_service::Client,
    ttid: TenantTimelineId,
    compression: bool,
}

impl LibpqClient {
    async fn new(
        connstring: &str,
        ttid: TenantTimelineId,
        compression: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: pageserver_client::page_service::Client::new(connstring.to_string()).await?,
            ttid,
            compression,
        })
    }
}

#[async_trait]
impl Client for LibpqClient {
    async fn basebackup(
        &mut self,
        lsn: Option<Lsn>,
    ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send + 'static>>> {
        let req = BasebackupRequest {
            tenant_id: self.ttid.tenant_id,
            timeline_id: self.ttid.timeline_id,
            lsn,
            gzip: self.compression,
        };
        let stream = self.inner.basebackup(&req).await?;
        Ok(Box::pin(StreamReader::new(
            stream.map_err(std::io::Error::other),
        )))
    }
}

/// A gRPC Pageserver client.
struct GrpcClient {
    inner: page_api::Client,
    compression: page_api::BaseBackupCompression,
}

impl GrpcClient {
    async fn new(
        connstring: &str,
        ttid: TenantTimelineId,
        compression: bool,
    ) -> anyhow::Result<Self> {
        let inner = page_api::Client::connect(
            connstring.to_string(),
            ttid.tenant_id,
            ttid.timeline_id,
            ShardIndex::unsharded(),
            None,
            None, // NB: uses payload compression
        )
        .await?;
        let compression = match compression {
            true => page_api::BaseBackupCompression::Gzip,
            false => page_api::BaseBackupCompression::None,
        };
        Ok(Self { inner, compression })
    }
}

#[async_trait]
impl Client for GrpcClient {
    async fn basebackup(
        &mut self,
        lsn: Option<Lsn>,
    ) -> anyhow::Result<Pin<Box<dyn AsyncRead + Send + 'static>>> {
        let req = page_api::GetBaseBackupRequest {
            lsn,
            replica: false,
            full: false,
            compression: self.compression,
        };
        Ok(Box::pin(self.inner.get_base_backup(req).await?))
    }
}
