use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tonic::metadata::AsciiMetadataValue;
use anyhow::Context;
use camino::Utf8PathBuf;
use pageserver_api::key::Key;
use pageserver_api::keyspace::KeySpaceAccum;
use pageserver_api::models::{PagestreamGetPageRequest, PagestreamRequest};
use pageserver_api::shard::TenantShardId;
use pageserver_client::page_service::PagestreamClient;
use rand::prelude::*;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;
use utils::id::TenantTimelineId;
use utils::id::TenantId;
use utils::id::TimelineId;
use utils::lsn::Lsn;
use futures::{
    future::BoxFuture,
    stream::FuturesOrdered,
    FutureExt, StreamExt,
};

use crate::util::tokio_thread_local_stats::AllThreadLocalStats;
use crate::util::{request_stats, tokio_thread_local_stats};

use async_trait::async_trait;
use rand::distributions::weighted::WeightedIndex;
use utils::shard::ShardIndex;

/// GetPage@LatestLSN, uniformly distributed across the compute-accessible keyspace.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "false")]
    grpc: bool,
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    /// Each client sends requests at the given rate.
    ///
    /// If a request takes too long and we should be issuing a new request already,
    /// we skip that request and account it as `MISSED`.
    #[clap(long)]
    per_client_rate: Option<usize>,
    /// Probability for sending `latest=true` in the request (uniform distribution).
    #[clap(long, default_value = "1")]
    req_latest_probability: f64,
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
    /// For large pageserver installations, enumerating the keyspace takes a lot of time.
    /// If specified, the specified path is used to maintain a cache of the keyspace enumeration result.
    /// The cache is tagged and auto-invalided by the tenant/timeline ids only.
    /// It doesn't get invalidated if the keyspace changes under the hood, e.g., due to new ingested data or compaction.
    #[clap(long)]
    keyspace_cache: Option<Utf8PathBuf>,
    /// Before starting the benchmark, live-reconfigure the pageserver to use the given
    /// [`pageserver_api::models::virtual_file::IoEngineKind`].
    #[clap(long)]
    set_io_engine: Option<pageserver_api::models::virtual_file::IoEngineKind>,

    /// Before starting the benchmark, live-reconfigure the pageserver to use specified io mode (buffered vs. direct).
    #[clap(long)]
    set_io_mode: Option<pageserver_api::models::virtual_file::IoMode>,

    /// Queue depth generated in each client.
    #[clap(long, default_value = "1")]
    queue_depth: NonZeroUsize,

    #[clap(long)]
    only_relnode: Option<u32>,

    targets: Option<Vec<TenantTimelineId>>,
}

/// State shared by all clients
#[derive(Debug)]
struct SharedState {
    start_work_barrier: tokio::sync::Barrier,
    live_stats: LiveStats,
}

#[derive(Debug, Default)]
struct LiveStats {
    completed_requests: AtomicU64,
    missed: AtomicU64,
}

impl LiveStats {
    fn request_done(&self) {
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
    fn missed(&self, n: u64) {
        self.missed.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
struct WorkerId {
    timeline: TenantTimelineId,
    num_client: usize, // from 0..args.num_clients
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

    if let Some(engine_str) = &args.set_io_engine {
        mgmt_api_client.put_io_engine(engine_str).await?;
    }

    if let Some(mode) = &args.set_io_mode {
        mgmt_api_client.put_io_mode(mode).await?;
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

    #[derive(serde::Deserialize)]
    struct KeyspaceCacheDe {
        tag: Vec<TenantTimelineId>,
        data: Vec<KeyRange>,
    }
    #[derive(serde::Serialize)]
    struct KeyspaceCacheSer<'a> {
        tag: &'a [TenantTimelineId],
        data: &'a [KeyRange],
    }
    let cache = args
        .keyspace_cache
        .as_ref()
        .map(|keyspace_cache_file| {
            let contents = match std::fs::read(keyspace_cache_file) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return anyhow::Ok(None);
                }
                x => x.context("read keyspace cache file")?,
            };
            let cache: KeyspaceCacheDe =
                serde_json::from_slice(&contents).context("deserialize cache file")?;
            let tag_ok = HashSet::<TenantTimelineId>::from_iter(cache.tag.into_iter())
                == HashSet::from_iter(timelines.iter().cloned());
            info!("keyspace cache file matches tag: {tag_ok}");
            anyhow::Ok(if tag_ok { Some(cache.data) } else { None })
        })
        .transpose()?
        .flatten();
    let all_ranges: Vec<KeyRange> = if let Some(cached) = cache {
        info!("using keyspace cache file");
        cached
    } else {
        let mut js = JoinSet::new();
        for timeline in &timelines {
            js.spawn({
                let mgmt_api_client = Arc::clone(&mgmt_api_client);
                let timeline = *timeline;
                async move {
                    let partitioning = mgmt_api_client
                        .keyspace(
                            TenantShardId::unsharded(timeline.tenant_id),
                            timeline.timeline_id,
                        )
                        .await?;
                    let lsn = partitioning.at_lsn;
                    let start = Instant::now();
                    let mut filtered = KeySpaceAccum::new();
                    // let's hope this is inlined and vectorized...
                    // TODO: turn this loop into a is_rel_block_range() function.
                    for r in partitioning.keys.ranges.iter() {
                        let mut i = r.start;
                        while i != r.end {
                            let mut include = true;
                            include &= i.is_rel_block_key();
                            if let Some(only_relnode) = args.only_relnode {
                                include &= i.is_rel_block_of_rel(only_relnode);
                            }
                            if include {
                                filtered.add_key(i);
                            }
                            i = i.next();
                        }
                    }
                    let filtered = filtered.to_keyspace();
                    let filter_duration = start.elapsed();

                    anyhow::Ok((
                        filter_duration,
                        filtered.ranges.into_iter().map(move |r| KeyRange {
                            timeline,
                            timeline_lsn: lsn,
                            start: r.start.to_i128(),
                            end: r.end.to_i128(),
                        }),
                    ))
                }
            });
        }
        let mut total_filter_duration = Duration::from_secs(0);
        let mut all_ranges: Vec<KeyRange> = Vec::new();
        while let Some(res) = js.join_next().await {
            let (filter_duration, range) = res.unwrap().unwrap();
            all_ranges.extend(range);
            total_filter_duration += filter_duration;
        }
        info!("filter duration: {}", total_filter_duration.as_secs_f64());
        if let Some(cachefile) = args.keyspace_cache.as_ref() {
            let cache = KeyspaceCacheSer {
                tag: &timelines,
                data: &all_ranges,
            };
            let bytes = serde_json::to_vec(&cache).context("serialize keyspace for cache file")?;
            std::fs::write(cachefile, bytes).context("write keyspace cache file to disk")?;
            info!("successfully wrote keyspace cache file");
        }
        all_ranges
    };

    let num_live_stats_dump = 1;
    let num_work_sender_tasks = args.num_clients.get() * timelines.len();
    let num_main_impl = 1;

    let shared_state = Arc::new(SharedState {
        start_work_barrier: tokio::sync::Barrier::new(
            num_live_stats_dump + num_work_sender_tasks + num_main_impl,
        ),
        live_stats: LiveStats::default(),
    });
    let cancel = CancellationToken::new();

    let ss = shared_state.clone();
    tokio::spawn({
        async move {
            ss.start_work_barrier.wait().await;
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let stats = &ss.live_stats;
                let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                let missed = stats.missed.swap(0, Ordering::Relaxed);
                let elapsed = start.elapsed();
                info!(
                    "RPS: {:.0}   MISSED: {:.0}",
                    completed_requests as f64 / elapsed.as_secs_f64(),
                    missed as f64 / elapsed.as_secs_f64()
                );
            }
        }
    });

    let rps_period = args
        .per_client_rate
        .map(|rps_limit| Duration::from_secs_f64(1.0 / (rps_limit as f64)));
    let make_worker: &dyn Fn(WorkerId) -> Pin<Box<dyn Send + Future<Output = ()>>> = &|worker_id| {
        let ss = shared_state.clone();
        let cancel = cancel.clone();
        let ranges: Vec<KeyRange> = all_ranges
            .iter()
            .filter(|r| r.timeline == worker_id.timeline)
            .cloned()
            .collect();
        let weights =
            rand::distributions::weighted::WeightedIndex::new(ranges.iter().map(|v| v.len()))
                .unwrap();

        Box::pin(async move {
            if args.grpc {
                let grpc = GrpcProtocol::new(
                    args.page_service_connstring.clone(),
                    worker_id.timeline.tenant_id,
                    worker_id.timeline.timeline_id).await;
                client_proto(args, grpc, worker_id, ss, cancel, rps_period, ranges, weights).await
            } else {
                let pg =  PgProtocol::new(
                    args.page_service_connstring.clone(),
                    worker_id.timeline.tenant_id,
                    worker_id.timeline.timeline_id).await;
                client_proto(args, pg, worker_id, ss, cancel, rps_period, ranges, weights).await
            }
        })
    };

    info!("spawning workers");
    let mut workers = JoinSet::new();
    for timeline in timelines.iter().cloned() {
        for num_client in 0..args.num_clients.get() {
            let worker_id = WorkerId {
                timeline,
                num_client,
            };
            workers.spawn(make_worker(worker_id));
        }
    }
    let workers = async move {
        while let Some(res) = workers.join_next().await {
            res.unwrap();
        }
    };

    info!("waiting for everything to become ready");
    shared_state.start_work_barrier.wait().await;
    info!("work started");
    if let Some(runtime) = args.runtime {
        tokio::time::sleep(runtime.into()).await;
        info!("runtime over, signalling cancellation");
        cancel.cancel();
        workers.await;
        info!("work sender exited");
    } else {
        workers.await;
        unreachable!("work sender never terminates");
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
/// Common interface for both Pg and Grpc versions.
#[async_trait]
trait Protocol: Send {
    /// Constructor/factory.
    async fn new(
        conn_string: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Self
    where
        Self: Sized;

    /// Fire off a “get page” request and store the start time.
    async fn add_to_inflight(
        &mut self,
        start: Instant,
        args: &Args,
        ranges: Vec<KeyRange>,
        weights: WeightedIndex<i128>,
    );

    /// Wait for the next response and return its start time.
    async fn get_start_time(&mut self) -> Instant;

    /// How many in-flight requests do we have?
    fn len(&self) -> usize;
}

///////////////////////////////////////////////////////////////////////////////
// PgProtocol
///////////////////////////////////////////////////////////////////////////////

struct PgProtocol {
    libpq_pagestream: PagestreamClient,
    libpq_vector: VecDeque<Instant>,
}

#[async_trait]
impl Protocol for PgProtocol {
    async fn new(
        conn_string: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Self {
        let client = pageserver_client::page_service::Client::new(conn_string)
            .await
            .unwrap()
            .pagestream(tenant_id, timeline_id)
            .await
            .unwrap();
        Self {
            libpq_pagestream: client,
            libpq_vector: VecDeque::new(),
        }
    }

    async fn add_to_inflight(
        &mut self,
        start: Instant,
        args: &Args,
        ranges: Vec<KeyRange>,
        weights: WeightedIndex<i128>,
    ) {
        // build your PagestreamGetPageRequest exactly as before…
        let req = {
            let mut rng = rand::thread_rng();
            let r = &ranges[weights.sample(&mut rng)];
            let key: i128 = rng.gen_range(r.start..r.end);
            let key = Key::from_i128(key);
            assert!(key.is_rel_block_key());
            let (rel_tag, block_no) = key.to_rel_block().unwrap();
            PagestreamGetPageRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: if rng.gen_bool(args.req_latest_probability) {
                        Lsn::MAX
                    } else {
                        r.timeline_lsn
                    },
                    not_modified_since: r.timeline_lsn,
                },
                rel: rel_tag,
                blkno: block_no,
            }
        };

        let _ = self.libpq_pagestream.getpage_send(req).await;
        self.libpq_vector.push_back(start);
    }

    async fn get_start_time(&mut self) -> Instant {
        let start = self.libpq_vector.pop_front().unwrap();
        let _ = self.libpq_pagestream.getpage_recv().await;
        start
    }

    fn len(&self) -> usize {
        self.libpq_vector.len()
    }
}

///////////////////////////////////////////////////////////////////////////////
// GrpcProtocol
///////////////////////////////////////////////////////////////////////////////
type GetPageFut = BoxFuture<'static, (Instant, Option<pageserver_client_grpc::PageserverClientError>)>;
struct GrpcProtocol {
    grpc_page_client: Arc<pageserver_client_grpc::PageserverClient>,
    grpc_vector: FuturesOrdered<GetPageFut>,
}

#[async_trait]
impl Protocol for GrpcProtocol {
    async fn new(
        conn_string: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Self {
        let shard_map = std::collections::HashMap::from([(
            ShardIndex::unsharded(),
            conn_string.clone(),
        )]);
        let tenant_ascii : AsciiMetadataValue = tenant_id.to_string().parse().unwrap();
        let timeline_ascii : AsciiMetadataValue = timeline_id.to_string().parse().unwrap();
        let client = pageserver_client_grpc::PageserverClient::new(
            tenant_ascii,
            timeline_ascii,
            None,
            shard_map,
        ).unwrap();
        Self {
            grpc_page_client: Arc::new(client),
            grpc_vector: FuturesOrdered::new(),
        }
    }

    async fn add_to_inflight(
        &mut self,
        start: Instant,
        args: &Args,
        ranges: Vec<KeyRange>,
        weights: WeightedIndex<i128>,
    ) {
        // build your GetPageRequest exactly as before…
        let req = {
            let mut rng = rand::thread_rng();
            let r = &ranges[weights.sample(&mut rng)];
            let key: i128 = rng.gen_range(r.start..r.end);
            let key = Key::from_i128(key);
            assert!(key.is_rel_block_key());
            let (rel_tag, block_no) = key.to_rel_block().unwrap();
            pageserver_page_api::GetPageRequest {
                request_id: 0,
                request_class: pageserver_page_api::GetPageClass::Normal,
                read_lsn: pageserver_page_api::ReadLsn {
                    request_lsn: if rng.gen_bool(args.req_latest_probability) {
                        Lsn::MAX
                    } else {
                        r.timeline_lsn
                    },
                    not_modified_since_lsn: Some(r.timeline_lsn),
                },
                rel: pageserver_page_api::RelTag {
                    spcnode: rel_tag.spcnode,
                    dbnode: rel_tag.dbnode,
                    relnode: rel_tag.relnode,
                    forknum: rel_tag.forknum,
                },
                block_numbers: vec![block_no].into(),
            }
        };

        let client_clone = self.grpc_page_client.clone();
        let getpage_fut : GetPageFut = async move {
            let result = client_clone.get_page(ShardIndex::unsharded(), req).await;
            match result {
                Ok(_) => {
                    (start, None)
                }
                Err(e) => {
                    (start, Some(e))
                }
            }
        }.boxed();
        self.grpc_vector.push_back(getpage_fut);
    }

    async fn get_start_time(&mut self) -> Instant {
        let (start, err) = self.grpc_vector.next().await.unwrap();
        if let Some(e) = err {
            tracing::error!("getpage request failed: {e}");
        }
        start
    }

    fn len(&self) -> usize {
        self.grpc_vector.len()
    }
}

async fn client_proto(
    args: &Args,
    mut protocol: impl Protocol,
    worker_id: WorkerId,
    shared_state: Arc<SharedState>,
    cancel: CancellationToken,
    rps_period: Option<Duration>,
    ranges: Vec<KeyRange>,
    weights: rand::distributions::weighted::WeightedIndex<i128>,
) {


    shared_state.start_work_barrier.wait().await;
    let client_start = Instant::now();
    let mut ticks_processed = 0;
    while !cancel.is_cancelled() {
        // Detect if a request took longer than the RPS rate
        if let Some(period) = &rps_period {
            let periods_passed_until_now =
                usize::try_from(client_start.elapsed().as_micros() / period.as_micros()).unwrap();

            if periods_passed_until_now > ticks_processed {
                shared_state
                    .live_stats
                    .missed((periods_passed_until_now - ticks_processed) as u64);
            }
            ticks_processed = periods_passed_until_now;
        }

        while protocol.len() < args.queue_depth.get() {
            let start = Instant::now();
            protocol.add_to_inflight(start, args, ranges.clone(), weights.clone()).await;
        }

        let start = protocol.get_start_time().await;
        let end = Instant::now();
        shared_state.live_stats.request_done();
        ticks_processed += 1;
        STATS.with(|stats| {
            stats
                .borrow()
                .lock()
                .unwrap()
                .observe(end.duration_since(start))
                .unwrap();
        });

        if let Some(period) = &rps_period {
            let next_at = client_start
                + Duration::from_micros(
                (ticks_processed) as u64 * u64::try_from(period.as_micros()).unwrap(),
            );
            tokio::time::sleep_until(next_at.into()).await;
        }
    }
}


