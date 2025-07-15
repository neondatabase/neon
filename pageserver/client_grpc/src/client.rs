use std::collections::HashMap;
use std::num::NonZero;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arc_swap::ArcSwap;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use tonic::codec::CompressionEncoding;
use tracing::{debug, instrument};
use utils::logging::warn_slow;

use crate::pool::{ChannelPool, ClientGuard, ClientPool, StreamGuard, StreamPool};
use crate::retry::Retry;
use crate::split::GetPageSplitter;
use compute_api::spec::PageserverProtocol;
use pageserver_api::shard::ShardStripeSize;
use pageserver_page_api as page_api;
use utils::id::{TenantId, TimelineId};
use utils::shard::{ShardCount, ShardIndex, ShardNumber};

/// Max number of concurrent clients per channel (i.e. TCP connection). New channels will be spun up
/// when full.
///
/// Normal requests are small, and we don't pipeline them, so we can afford a large number of
/// streams per connection.
///
/// TODO: tune all of these constants, and consider making them configurable.
const MAX_CLIENTS_PER_CHANNEL: NonZero<usize> = NonZero::new(64).unwrap();

/// Max number of concurrent bulk GetPage streams per channel (i.e. TCP connection). These use a
/// dedicated channel pool with a lower client limit, to avoid TCP-level head-of-line blocking and
/// transmission delays. This also concentrates large window sizes on a smaller set of
/// streams/connections, presumably reducing memory use.
const MAX_BULK_CLIENTS_PER_CHANNEL: NonZero<usize> = NonZero::new(16).unwrap();

/// The batch size threshold at which a GetPage request will use the bulk stream pool.
///
/// The gRPC initial window size is 64 KB. Each page is 8 KB, so let's avoid increasing the window
/// size for the normal stream pool, and route requests for >= 5 pages (>32 KB) to the bulk pool.
const BULK_THRESHOLD_BATCH_SIZE: usize = 5;

/// The overall request call timeout, including retries and pool acquisition.
/// TODO: should we retry forever? Should the caller decide?
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// The per-request (retry attempt) timeout, including any lazy connection establishment.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The initial request retry backoff duration. The first retry does not back off.
/// TODO: use a different backoff for ResourceExhausted (rate limiting)? Needs server support.
const BASE_BACKOFF: Duration = Duration::from_millis(5);

/// The maximum request retry backoff duration.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Threshold and interval for warning about slow operation.
const SLOW_THRESHOLD: Duration = Duration::from_secs(3);

/// A rich Pageserver gRPC client for a single tenant timeline. This client is more capable than the
/// basic `page_api::Client` gRPC client, and supports:
///
/// * Sharded tenants across multiple Pageservers.
/// * Pooling of connections, clients, and streams for efficient resource use.
/// * Concurrent use by many callers.
/// * Internal handling of GetPage bidirectional streams.
/// * Automatic retries.
/// * Observability.
///
/// The client has dedicated connection/client/stream pools per shard, for resource reuse. These
/// pools are unbounded: we allow scaling out as many concurrent streams as needed to serve all
/// concurrent callers, which mostly eliminates head-of-line blocking. Idle streams are fairly
/// cheap: the server task currently uses 26 KB of memory, so we can comfortably fit 100,000
/// concurrent idle streams (2.5 GB memory). The worst case degenerates to the old libpq case with
/// one stream per backend, but without the TCP connection overhead. In the common case we expect
/// significantly lower stream counts due to stream sharing, driven e.g. by idle backends, LFC hits,
/// read coalescing, sharding (backends typically only talk to one shard at a time), etc.
///
/// TODO: this client does not support base backups or LSN leases, as these are only used by
/// compute_ctl. Consider adding this, but LSN leases need concurrent requests on all shards.
pub struct PageserverClient {
    /// The tenant ID.
    tenant_id: TenantId,
    /// The timeline ID.
    timeline_id: TimelineId,
    /// The JWT auth token for this tenant, if any.
    auth_token: Option<String>,
    /// The compression to use, if any.
    compression: Option<CompressionEncoding>,
    /// The shards for this tenant.
    shards: ArcSwap<Shards>,
}

impl PageserverClient {
    /// Creates a new Pageserver client for a given tenant and timeline. Uses the Pageservers given
    /// in the shard spec, which must be complete and must use gRPC URLs.
    pub fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_spec: ShardSpec,
        auth_token: Option<String>,
        compression: Option<CompressionEncoding>,
    ) -> anyhow::Result<Self> {
        let shards = Shards::new(
            tenant_id,
            timeline_id,
            shard_spec,
            auth_token.clone(),
            compression,
        )?;
        Ok(Self {
            tenant_id,
            timeline_id,
            auth_token,
            compression,
            shards: ArcSwap::new(Arc::new(shards)),
        })
    }

    /// Updates the shards from the given shard spec. In-flight requests will complete using the
    /// existing shards, but may retry with the new shards if they fail.
    ///
    /// TODO: verify that in-flight requests are allowed to complete, and that the old pools are
    /// properly spun down and dropped afterwards.
    pub fn update_shards(&self, shard_spec: ShardSpec) -> anyhow::Result<()> {
        // Validate the shard spec. We should really use `ArcSwap::rcu` for this, to avoid races
        // with concurrent updates, but that involves creating a new `Shards` on every attempt,
        // which spins up a bunch of Tokio tasks and such. These should already be checked elsewhere
        // in the stack, and if they're violated then we already have problems elsewhere, so a
        // best-effort but possibly-racy check is okay here.
        let old = self.shards.load_full();
        if shard_spec.count < old.count {
            return Err(anyhow!(
                "can't reduce shard count from {} to {}",
                old.count,
                shard_spec.count
            ));
        }
        if !old.count.is_unsharded() && shard_spec.stripe_size != old.stripe_size {
            return Err(anyhow!(
                "can't change stripe size from {} to {}",
                old.stripe_size,
                shard_spec.stripe_size
            ));
        }

        let shards = Shards::new(
            self.tenant_id,
            self.timeline_id,
            shard_spec,
            self.auth_token.clone(),
            self.compression,
        )?;
        self.shards.store(Arc::new(shards));
        Ok(())
    }

    /// Returns the total size of a database, as # of bytes.
    #[instrument(skip_all, fields(db_oid=%req.db_oid, lsn=%req.read_lsn))]
    pub async fn get_db_size(
        &self,
        req: page_api::GetDbSizeRequest,
    ) -> tonic::Result<page_api::GetDbSizeResponse> {
        debug!("sending request: {req:?}");
        let resp = Self::with_retries(CALL_TIMEOUT, async |_| {
            // Relation metadata is only available on shard 0.
            let mut client = self.shards.load_full().get_zero().client().await?;
            Self::with_timeout(REQUEST_TIMEOUT, client.get_db_size(req)).await
        })
        .await?;
        debug!("received response: {resp:?}");
        Ok(resp)
    }

    /// Fetches pages. The `request_id` must be unique across all in-flight requests, and the
    /// `attempt` must be 0 (incremented on retry). Automatically splits requests that straddle
    /// shard boundaries, and assembles the responses.
    ///
    /// Unlike `page_api::Client`, this automatically converts `status_code` into `tonic::Status`
    /// errors. All responses will have `GetPageStatusCode::Ok`.
    #[instrument(skip_all, fields(
        req_id = %req.request_id,
        class = %req.request_class,
        rel = %req.rel,
        blkno = %req.block_numbers[0],
        blks = %req.block_numbers.len(),
        lsn = %req.read_lsn,
    ))]
    pub async fn get_page(
        &self,
        req: page_api::GetPageRequest,
    ) -> tonic::Result<page_api::GetPageResponse> {
        // Make sure we have at least one page.
        if req.block_numbers.is_empty() {
            return Err(tonic::Status::invalid_argument("no block number"));
        }
        // The request attempt must be 0. The client will increment it internally.
        if req.request_id.attempt != 0 {
            return Err(tonic::Status::invalid_argument("request attempt must be 0"));
        }

        debug!("sending request: {req:?}");

        // The shards may change while we're fetching pages. We execute the request using a stable
        // view of the shards (especially important for requests that span shards), but retry the
        // top-level (pre-split) request to pick up shard changes. This can lead to unnecessary
        // retries and re-splits in some cases where requests span shards, but these are expected to
        // be rare.
        //
        // TODO: the gRPC server and client doesn't yet properly support shard splits. Revisit this
        // once we figure out how to handle these.
        let resp = Self::with_retries(CALL_TIMEOUT, async |attempt| {
            let mut req = req.clone();
            req.request_id.attempt = attempt as u32;
            let shards = self.shards.load_full();
            Self::with_timeout(REQUEST_TIMEOUT, Self::get_page_with_shards(req, &shards)).await
        })
        .await?;

        debug!("received response: {resp:?}");
        Ok(resp)
    }

    /// Fetches pages using the given shards. This uses a stable view of the shards, regardless of
    /// concurrent shard updates. Does not retry internally, but is retried by `get_page()`.
    async fn get_page_with_shards(
        req: page_api::GetPageRequest,
        shards: &Shards,
    ) -> tonic::Result<page_api::GetPageResponse> {
        // Fast path: request is for a single shard.
        if let Some(shard_id) =
            GetPageSplitter::for_single_shard(&req, shards.count, shards.stripe_size)
        {
            return Self::get_page_with_shard(req, shards.get(shard_id)?).await;
        }

        // Request spans multiple shards. Split it, dispatch concurrent per-shard requests, and
        // reassemble the responses.
        let mut splitter = GetPageSplitter::split(req, shards.count, shards.stripe_size);

        let mut shard_requests = FuturesUnordered::new();
        for (shard_id, shard_req) in splitter.drain_requests() {
            let future = Self::get_page_with_shard(shard_req, shards.get(shard_id)?)
                .map(move |result| result.map(|resp| (shard_id, resp)));
            shard_requests.push(future);
        }

        while let Some((shard_id, shard_response)) = shard_requests.next().await.transpose()? {
            splitter.add_response(shard_id, shard_response)?;
        }

        splitter.get_response()
    }

    /// Fetches pages on the given shard. Does not retry internally.
    async fn get_page_with_shard(
        req: page_api::GetPageRequest,
        shard: &Shard,
    ) -> tonic::Result<page_api::GetPageResponse> {
        let mut stream = shard.stream(Self::is_bulk(&req)).await?;
        let resp = stream.send(req.clone()).await?;

        // Convert per-request errors into a tonic::Status.
        if resp.status_code != page_api::GetPageStatusCode::Ok {
            return Err(tonic::Status::new(
                resp.status_code.into(),
                resp.reason.unwrap_or_else(|| String::from("unknown error")),
            ));
        }

        // Check that we received the expected pages.
        if req.rel != resp.rel {
            return Err(tonic::Status::internal(format!(
                "shard {} returned wrong relation, expected {} got {}",
                shard.id, req.rel, resp.rel
            )));
        }
        if !req
            .block_numbers
            .iter()
            .copied()
            .eq(resp.pages.iter().map(|p| p.block_number))
        {
            return Err(tonic::Status::internal(format!(
                "shard {} returned wrong pages, expected {:?} got {:?}",
                shard.id,
                req.block_numbers,
                resp.pages
                    .iter()
                    .map(|page| page.block_number)
                    .collect::<Vec<_>>()
            )));
        }

        Ok(resp)
    }

    /// Returns the size of a relation, as # of blocks.
    #[instrument(skip_all, fields(rel=%req.rel, lsn=%req.read_lsn))]
    pub async fn get_rel_size(
        &self,
        req: page_api::GetRelSizeRequest,
    ) -> tonic::Result<page_api::GetRelSizeResponse> {
        debug!("sending request: {req:?}");
        let resp = Self::with_retries(CALL_TIMEOUT, async |_| {
            // Relation metadata is only available on shard 0.
            let mut client = self.shards.load_full().get_zero().client().await?;
            Self::with_timeout(REQUEST_TIMEOUT, client.get_rel_size(req)).await
        })
        .await?;
        debug!("received response: {resp:?}");
        Ok(resp)
    }

    /// Fetches an SLRU segment.
    #[instrument(skip_all, fields(kind=%req.kind, segno=%req.segno, lsn=%req.read_lsn))]
    pub async fn get_slru_segment(
        &self,
        req: page_api::GetSlruSegmentRequest,
    ) -> tonic::Result<page_api::GetSlruSegmentResponse> {
        debug!("sending request: {req:?}");
        let resp = Self::with_retries(CALL_TIMEOUT, async |_| {
            // SLRU segments are only available on shard 0.
            let mut client = self.shards.load_full().get_zero().client().await?;
            Self::with_timeout(REQUEST_TIMEOUT, client.get_slru_segment(req)).await
        })
        .await?;
        debug!("received response: {resp:?}");
        Ok(resp)
    }

    /// Runs the given async closure with retries up to the given timeout. Only certain gRPC status
    /// codes are retried, see [`Retry::should_retry`]. Returns `DeadlineExceeded` on timeout.
    async fn with_retries<T, F, O>(timeout: Duration, f: F) -> tonic::Result<T>
    where
        F: FnMut(usize) -> O, // pass attempt number, starting at 0
        O: Future<Output = tonic::Result<T>>,
    {
        Retry {
            timeout: Some(timeout),
            base_backoff: BASE_BACKOFF,
            max_backoff: MAX_BACKOFF,
        }
        .with(f)
        .await
    }

    /// Runs the given future with a timeout. Returns `DeadlineExceeded` on timeout.
    async fn with_timeout<T>(
        timeout: Duration,
        f: impl Future<Output = tonic::Result<T>>,
    ) -> tonic::Result<T> {
        let started = Instant::now();
        tokio::time::timeout(timeout, f).await.map_err(|_| {
            tonic::Status::deadline_exceeded(format!(
                "request timed out after {:.3}s",
                started.elapsed().as_secs_f64()
            ))
        })?
    }

    /// Returns true if the request is considered a bulk request and should use the bulk pool.
    fn is_bulk(req: &page_api::GetPageRequest) -> bool {
        req.block_numbers.len() >= BULK_THRESHOLD_BATCH_SIZE
    }
}

/// Shard specification for a PageserverClient.
pub struct ShardSpec {
    /// Maps shard indices to gRPC URLs.
    ///
    /// INVARIANT: every shard 0..count is present, and shard 0 is always present.
    /// INVARIANT: every URL is valid and uses grpc:// scheme.
    urls: HashMap<ShardIndex, String>,
    /// The shard count.
    ///
    /// NB: this is 0 for unsharded tenants, following `ShardIndex::unsharded()` convention.
    count: ShardCount,
    /// The stripe size for these shards.
    stripe_size: ShardStripeSize,
}

impl ShardSpec {
    /// Creates a new shard spec with the given URLs and stripe size. All shards must be given.
    /// The stripe size may be omitted for unsharded tenants.
    pub fn new(
        urls: HashMap<ShardIndex, String>,
        stripe_size: Option<ShardStripeSize>,
    ) -> anyhow::Result<Self> {
        // Compute the shard count.
        let count = match urls.len() {
            0 => return Err(anyhow!("no shards provided")),
            1 => ShardCount::new(0), // NB: unsharded tenants use 0, like `ShardIndex::unsharded()`
            n if n > u8::MAX as usize => return Err(anyhow!("too many shards: {n}")),
            n => ShardCount::new(n as u8),
        };

        // Determine the stripe size. It doesn't matter for unsharded tenants.
        if stripe_size.is_none() && !count.is_unsharded() {
            return Err(anyhow!("stripe size must be given for sharded tenants"));
        }
        let stripe_size = stripe_size.unwrap_or_default();

        // Validate the shard spec.
        for (shard_id, url) in &urls {
            // The shard index must match the computed shard count, even for unsharded tenants.
            if shard_id.shard_count != count {
                return Err(anyhow!("invalid shard index {shard_id}, expected {count}"));
            }
            // The shard index' number and count must be consistent.
            if !shard_id.is_unsharded() && shard_id.shard_number.0 >= shard_id.shard_count.0 {
                return Err(anyhow!("invalid shard index {shard_id}"));
            }
            // The above conditions guarantee that we have all shards 0..count: len() matches count,
            // shard number < count, and numbers are unique (via hashmap).

            // Validate the URL.
            if PageserverProtocol::from_connstring(url)? != PageserverProtocol::Grpc {
                return Err(anyhow!("invalid shard URL {url}: must use gRPC"));
            }
        }

        Ok(Self {
            urls,
            count,
            stripe_size,
        })
    }
}

/// Tracks the tenant's shards.
struct Shards {
    /// Shards by shard index.
    ///
    /// INVARIANT: every shard 0..count is present.
    /// INVARIANT: shard 0 is always present.
    by_index: HashMap<ShardIndex, Shard>,
    /// The shard count.
    ///
    /// NB: this is 0 for unsharded tenants, following `ShardIndex::unsharded()` convention.
    count: ShardCount,
    /// The stripe size. Only used for sharded tenants.
    stripe_size: ShardStripeSize,
}

impl Shards {
    /// Creates a new set of shards based on a shard spec.
    fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_spec: ShardSpec,
        auth_token: Option<String>,
        compression: Option<CompressionEncoding>,
    ) -> anyhow::Result<Self> {
        // NB: the shard spec has already been validated when constructed.
        let mut shards = HashMap::with_capacity(shard_spec.urls.len());
        for (shard_id, url) in shard_spec.urls {
            shards.insert(
                shard_id,
                Shard::new(
                    url,
                    tenant_id,
                    timeline_id,
                    shard_id,
                    auth_token.clone(),
                    compression,
                )?,
            );
        }

        Ok(Self {
            by_index: shards,
            count: shard_spec.count,
            stripe_size: shard_spec.stripe_size,
        })
    }

    /// Looks up the given shard.
    #[allow(clippy::result_large_err)] // TODO: check perf impact
    fn get(&self, shard_id: ShardIndex) -> tonic::Result<&Shard> {
        self.by_index
            .get(&shard_id)
            .ok_or_else(|| tonic::Status::not_found(format!("unknown shard {shard_id}")))
    }

    /// Returns shard 0.
    fn get_zero(&self) -> &Shard {
        self.get(ShardIndex::new(ShardNumber(0), self.count))
            .expect("always present")
    }
}

/// A single shard. Has dedicated resource pools with the following structure:
///
/// * Channel pool: MAX_CLIENTS_PER_CHANNEL.
///   * Client pool: unbounded.
///     * Stream pool: unbounded.
/// * Bulk channel pool: MAX_BULK_CLIENTS_PER_CHANNEL.
///   * Bulk client pool: unbounded.
///     * Bulk stream pool: unbounded.
///
/// We use a separate bulk channel pool with a lower concurrency limit for large batch requests.
/// This avoids TCP-level head-of-line blocking, and also concentrates large window sizes on a
/// smaller set of streams/connections, which presumably reduces memory use. Neither of these pools
/// are bounded, nor do they pipeline requests, so the latency characteristics should be mostly
/// similar (except for TCP transmission time).
///
/// TODO: since we never use bounded pools, we could consider removing the pool limiters. However,
/// the code is fairly trivial, so we may as well keep them around for now in case we need them.
struct Shard {
    /// The shard ID.
    id: ShardIndex,
    /// Unary gRPC client pool.
    client_pool: Arc<ClientPool>,
    /// GetPage stream pool.
    stream_pool: Arc<StreamPool>,
    /// GetPage stream pool for bulk requests.
    bulk_stream_pool: Arc<StreamPool>,
}

impl Shard {
    /// Creates a new shard. It has its own dedicated resource pools.
    fn new(
        url: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
        compression: Option<CompressionEncoding>,
    ) -> anyhow::Result<Self> {
        // Shard pools for unary requests and non-bulk GetPage requests.
        let client_pool = ClientPool::new(
            ChannelPool::new(url.clone(), MAX_CLIENTS_PER_CHANNEL)?,
            tenant_id,
            timeline_id,
            shard_id,
            auth_token.clone(),
            compression,
            None, // unbounded
        );
        let stream_pool = StreamPool::new(client_pool.clone(), None); // unbounded

        // Bulk GetPage stream pool for large batches (prefetches, sequential scans, vacuum, etc.).
        let bulk_stream_pool = StreamPool::new(
            ClientPool::new(
                ChannelPool::new(url, MAX_BULK_CLIENTS_PER_CHANNEL)?,
                tenant_id,
                timeline_id,
                shard_id,
                auth_token,
                compression,
                None, // unbounded,
            ),
            None, // unbounded
        );

        Ok(Self {
            id: shard_id,
            client_pool,
            stream_pool,
            bulk_stream_pool,
        })
    }

    /// Returns a pooled client for this shard.
    #[instrument(skip_all)]
    async fn client(&self) -> tonic::Result<ClientGuard> {
        warn_slow(
            "client pool acquisition",
            SLOW_THRESHOLD,
            pin!(self.client_pool.get()),
        )
        .await
    }

    /// Returns a pooled stream for this shard. If `bulk` is `true`, uses the dedicated bulk pool.
    #[instrument(skip_all, fields(bulk))]
    async fn stream(&self, bulk: bool) -> tonic::Result<StreamGuard> {
        let pool = match bulk {
            false => &self.stream_pool,
            true => &self.bulk_stream_pool,
        };
        warn_slow("stream pool acquisition", SLOW_THRESHOLD, pin!(pool.get())).await
    }
}
