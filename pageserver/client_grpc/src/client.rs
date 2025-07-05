use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use tracing::instrument;

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
/// TODO: tune all of these constants, and consider making them configurable.
/// TODO: consider separate limits for unary and streaming clients, so we don't fill up channels
/// with only streams.
const MAX_CLIENTS_PER_CHANNEL: NonZero<usize> = NonZero::new(16).unwrap();

/// Max number of concurrent unary request clients per shard.
const MAX_UNARY_CLIENTS: NonZero<usize> = NonZero::new(64).unwrap();

/// Max number of concurrent GetPage streams per shard. The max number of concurrent GetPage
/// requests is given by `MAX_STREAMS * MAX_STREAM_QUEUE_DEPTH`.
const MAX_STREAMS: NonZero<usize> = NonZero::new(64).unwrap();

/// Max number of pipelined requests per stream.
const MAX_STREAM_QUEUE_DEPTH: NonZero<usize> = NonZero::new(2).unwrap();

/// Max number of concurrent bulk GetPage streams per shard, used e.g. for prefetches. Because these
/// are more throughput-oriented, we have a smaller limit but higher queue depth.
const MAX_BULK_STREAMS: NonZero<usize> = NonZero::new(16).unwrap();

/// Max number of pipelined requests per bulk stream. These are more throughput-oriented and thus
/// get a larger queue depth.
const MAX_BULK_STREAM_QUEUE_DEPTH: NonZero<usize> = NonZero::new(4).unwrap();

/// A rich Pageserver gRPC client for a single tenant timeline. This client is more capable than the
/// basic `page_api::Client` gRPC client, and supports:
///
/// * Sharded tenants across multiple Pageservers.
/// * Pooling of connections, clients, and streams for efficient resource use.
/// * Concurrent use by many callers.
/// * Internal handling of GetPage bidirectional streams, with pipelining and error handling.
/// * Automatic retries.
/// * Observability.
///
/// TODO: this client does not support base backups or LSN leases, as these are only used by
/// compute_ctl. Consider adding this, but LSN leases need concurrent requests on all shards.
pub struct PageserverClient {
    // TODO: support swapping out the shard map, e.g. via an ArcSwap.
    shards: Shards,
    retry: Retry,
}

impl PageserverClient {
    /// Creates a new Pageserver client for a given tenant and timeline. Uses the Pageservers given
    /// in the shard map, which must be complete and must use gRPC URLs.
    pub fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_map: HashMap<ShardIndex, String>,
        stripe_size: ShardStripeSize,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        let shards = Shards::new(tenant_id, timeline_id, shard_map, stripe_size, auth_token)?;
        Ok(Self {
            shards,
            retry: Retry,
        })
    }

    /// Returns whether a relation exists.
    #[instrument(skip_all, fields(rel=%req.rel, lsn=%req.read_lsn))]
    pub async fn check_rel_exists(
        &self,
        req: page_api::CheckRelExistsRequest,
    ) -> tonic::Result<page_api::CheckRelExistsResponse> {
        self.retry
            .with(async || {
                // Relation metadata is only available on shard 0.
                let mut client = self.shards.get_zero().client().await?;
                client.check_rel_exists(req).await
            })
            .await
    }

    /// Returns the total size of a database, as # of bytes.
    #[instrument(skip_all, fields(db_oid=%req.db_oid, lsn=%req.read_lsn))]
    pub async fn get_db_size(
        &self,
        req: page_api::GetDbSizeRequest,
    ) -> tonic::Result<page_api::GetDbSizeResponse> {
        self.retry
            .with(async || {
                // Relation metadata is only available on shard 0.
                let mut client = self.shards.get_zero().client().await?;
                client.get_db_size(req).await
            })
            .await
    }

    /// Fetches pages. The `request_id` must be unique across all in-flight requests. Automatically
    /// splits requests that straddle shard boundaries, and assembles the responses.
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

        // Fast path: request is for a single shard.
        if let Some(shard_id) =
            GetPageSplitter::is_single_shard(&req, self.shards.count, self.shards.stripe_size)
        {
            return self.get_page_for_shard(shard_id, req).await;
        }

        // Request spans multiple shards. Split it, dispatch concurrent per-shard requests, and
        // reassemble the responses.
        //
        // TODO: when we support shard map updates, we need to detect when it changes and re-split
        // the request on errors.
        let mut splitter = GetPageSplitter::split(req, self.shards.count, self.shards.stripe_size);

        let mut shard_requests: FuturesUnordered<_> = splitter
            .drain_requests()
            .map(|(shard_id, shard_req)| {
                // NB: each request will retry internally.
                self.get_page_for_shard(shard_id, shard_req)
                    .map(move |result| result.map(|resp| (shard_id, resp)))
            })
            .collect();

        while let Some((shard_id, shard_response)) = shard_requests.next().await.transpose()? {
            splitter.add_response(shard_id, shard_response)?;
        }

        splitter.assemble_response()
    }

    /// Fetches pages that belong to the given shard.
    #[instrument(skip_all, fields(shard = %shard_id))]
    async fn get_page_for_shard(
        &self,
        shard_id: ShardIndex,
        req: page_api::GetPageRequest,
    ) -> tonic::Result<page_api::GetPageResponse> {
        let resp = self
            .retry
            .with(async || {
                let stream = self
                    .shards
                    .get(shard_id)?
                    .stream(req.request_class.is_bulk())
                    .await;
                let resp = stream.send(req.clone()).await?;

                // Convert per-request errors into a tonic::Status.
                if resp.status_code != page_api::GetPageStatusCode::Ok {
                    return Err(tonic::Status::new(
                        resp.status_code.into(),
                        resp.reason.unwrap_or_else(|| String::from("unknown error")),
                    ));
                }

                Ok(resp)
            })
            .await?;

        // Make sure we got the right number of pages.
        // NB: check outside of the retry loop, since we don't want to retry this.
        let (expected, actual) = (req.block_numbers.len(), resp.page_images.len());
        if expected != actual {
            return Err(tonic::Status::internal(format!(
                "expected {expected} pages for shard {shard_id}, got {actual}",
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
        self.retry
            .with(async || {
                // Relation metadata is only available on shard 0.
                let mut client = self.shards.get_zero().client().await?;
                client.get_rel_size(req).await
            })
            .await
    }

    /// Fetches an SLRU segment.
    #[instrument(skip_all, fields(kind=%req.kind, segno=%req.segno, lsn=%req.read_lsn))]
    pub async fn get_slru_segment(
        &self,
        req: page_api::GetSlruSegmentRequest,
    ) -> tonic::Result<page_api::GetSlruSegmentResponse> {
        self.retry
            .with(async || {
                // SLRU segments are only available on shard 0.
                let mut client = self.shards.get_zero().client().await?;
                client.get_slru_segment(req).await
            })
            .await
    }
}

/// Tracks the tenant's shards.
struct Shards {
    /// The shard count.
    ///
    /// NB: this is 0 for unsharded tenants, following `ShardIndex::unsharded()` convention.
    count: ShardCount,
    /// The stripe size. Only used for sharded tenants.
    stripe_size: ShardStripeSize,
    /// Shards by shard index.
    ///
    /// NB: unsharded tenants use count 0, like `ShardIndex::unsharded()`.
    ///
    /// INVARIANT: every shard 0..count is present.
    /// INVARIANT: shard 0 is always present.
    map: HashMap<ShardIndex, Shard>,
}

impl Shards {
    /// Creates a new set of shards based on a shard map.
    fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_map: HashMap<ShardIndex, String>,
        stripe_size: ShardStripeSize,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        let count = match shard_map.len() {
            0 => return Err(anyhow!("no shards provided")),
            1 => ShardCount::new(0), // NB: unsharded tenants use 0, like `ShardIndex::unsharded()`
            n if n > u8::MAX as usize => return Err(anyhow!("too many shards: {n}")),
            n => ShardCount::new(n as u8),
        };

        let mut map = HashMap::new();
        for (shard_id, url) in shard_map {
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
            let shard = Shard::new(url, tenant_id, timeline_id, shard_id, auth_token.clone())?;
            map.insert(shard_id, shard);
        }

        Ok(Self {
            count,
            stripe_size,
            map,
        })
    }

    /// Looks up the given shard.
    #[allow(clippy::result_large_err)] // TODO: check perf impact
    fn get(&self, shard_id: ShardIndex) -> tonic::Result<&Shard> {
        self.map
            .get(&shard_id)
            .ok_or_else(|| tonic::Status::not_found(format!("unknown shard {shard_id}")))
    }

    /// Returns shard 0.
    fn get_zero(&self) -> &Shard {
        self.get(ShardIndex::new(ShardNumber(0), self.count))
            .expect("always present")
    }
}

/// A single shard. Uses dedicated resource pools with the following structure:
///
/// * Channel pool: unbounded.
///   * Unary client pool: MAX_UNARY_CLIENTS.
///   * Stream client pool: unbounded.
///     * Stream pool: MAX_STREAMS and MAX_STREAM_QUEUE_DEPTH.
/// * Bulk channel pool: unbounded.
///   * Bulk client pool: unbounded.
///     * Bulk stream pool: MAX_BULK_STREAMS and MAX_BULK_STREAM_QUEUE_DEPTH.
struct Shard {
    /// Unary gRPC client pool.
    client_pool: Arc<ClientPool>,
    /// GetPage stream pool.
    stream_pool: Arc<StreamPool>,
    /// GetPage stream pool for bulk requests, e.g. prefetches.
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
    ) -> anyhow::Result<Self> {
        // Sanity-check that the URL uses gRPC.
        if PageserverProtocol::from_connstring(&url)? != PageserverProtocol::Grpc {
            return Err(anyhow!("invalid shard URL {url}: must use gRPC"));
        }

        // Common channel pool for unary and stream requests. Bounded by client/stream pools.
        let channel_pool = ChannelPool::new(url.clone(), MAX_CLIENTS_PER_CHANNEL)?;

        // Client pool for unary requests.
        let client_pool = ClientPool::new(
            channel_pool.clone(),
            tenant_id,
            timeline_id,
            shard_id,
            auth_token.clone(),
            Some(MAX_UNARY_CLIENTS),
        );

        // GetPage stream pool. Uses a dedicated client pool to avoid starving out unary clients,
        // but shares a channel pool with it (as it's unbounded).
        let stream_pool = StreamPool::new(
            ClientPool::new(
                channel_pool.clone(),
                tenant_id,
                timeline_id,
                shard_id,
                auth_token.clone(),
                None, // unbounded, limited by stream pool
            ),
            Some(MAX_STREAMS),
            MAX_STREAM_QUEUE_DEPTH,
        );

        // Bulk GetPage stream pool, e.g. for prefetches. Uses dedicated channel/client/stream pools
        // to avoid head-of-line blocking of latency-sensitive requests.
        let bulk_stream_pool = StreamPool::new(
            ClientPool::new(
                ChannelPool::new(url, MAX_CLIENTS_PER_CHANNEL)?,
                tenant_id,
                timeline_id,
                shard_id,
                auth_token,
                None, // unbounded, limited by stream pool
            ),
            Some(MAX_BULK_STREAMS),
            MAX_BULK_STREAM_QUEUE_DEPTH,
        );

        Ok(Self {
            client_pool,
            stream_pool,
            bulk_stream_pool,
        })
    }

    /// Returns a pooled client for this shard.
    async fn client(&self) -> tonic::Result<ClientGuard> {
        self.client_pool
            .get()
            .await
            .map_err(|err| tonic::Status::internal(format!("failed to get client: {err}")))
    }

    /// Returns a pooled stream for this shard. If `bulk` is `true`, uses the dedicated bulk stream
    /// pool (e.g. for prefetches).
    async fn stream(&self, bulk: bool) -> StreamGuard {
        match bulk {
            false => self.stream_pool.get().await,
            true => self.bulk_stream_pool.get().await,
        }
    }
}
