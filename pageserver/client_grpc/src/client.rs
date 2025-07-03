use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, ensure};
use pageserver_page_api as page_api;
use tokio_util::sync::CancellationToken;
use utils::backoff;
use utils::id::{TenantId, TimelineId};
use utils::shard::{ShardCount, ShardIndex, ShardNumber};

use crate::pool::{ChannelPool, ClientGuard, ClientPool, StreamGuard, StreamPool};

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
/// TODO: this client does not support base backups or LSN leases, as these are only used by
/// compute_ctl. Consider adding this, but LSN leases need concurrent requests on all shards.
pub struct PageserverClient {
    shards: Shards,
}

impl PageserverClient {
    /// Creates a new Pageserver client.
    pub fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_map: HashMap<ShardIndex, String>,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        let shards = Shards::new(tenant_id, timeline_id, shard_map, auth_token)?;
        Ok(Self { shards })
    }

    /// Returns whether a relation exists.
    pub async fn check_rel_exists(
        &self,
        req: page_api::CheckRelExistsRequest,
    ) -> tonic::Result<page_api::CheckRelExistsResponse> {
        self.with_retries("check_rel_exists", async || {
            // Relation metadata is only available on shard 0.
            let mut client = self.shards.get_zero().client().await?;
            client.check_rel_exists(req).await
        })
        .await
    }

    /// Returns the total size of a database, as # of bytes.
    pub async fn get_db_size(
        &self,
        req: page_api::GetDbSizeRequest,
    ) -> tonic::Result<page_api::GetDbSizeResponse> {
        self.with_retries("get_db_size", async || {
            // Relation metadata is only available on shard 0.
            let mut client = self.shards.get_zero().client().await?;
            client.get_db_size(req).await
        })
        .await
    }

    /// Fetches a page. The `request_id` must be unique across all in-flight requests.
    ///
    /// Unlike the `page_api::Client`, this client automatically converts `status_code` into
    /// `tonic::Status` errors. All responses will have `GetPageStatusCode::Ok`.
    pub async fn get_page(
        &self,
        req: page_api::GetPageRequest,
    ) -> tonic::Result<page_api::GetPageResponse> {
        // TODO: support multiple shards.
        let shard_id = ShardIndex::unsharded();

        self.with_retries("get_page", async || {
            let stream = self.shards.get(shard_id)?.stream().await;
            let resp = stream.send(req.clone()).await?;

            if resp.status_code != page_api::GetPageStatusCode::Ok {
                return Err(tonic::Status::new(
                    resp.status_code.into(),
                    resp.reason.unwrap_or_else(|| String::from("unknown error")),
                ));
            }

            Ok(resp)
        })
        .await
    }

    /// Returns the size of a relation, as # of blocks.
    pub async fn get_rel_size(
        &self,
        req: page_api::GetRelSizeRequest,
    ) -> tonic::Result<page_api::GetRelSizeResponse> {
        self.with_retries("get_rel_size", async || {
            // Relation metadata is only available on shard 0.
            let mut client = self.shards.get_zero().client().await?;
            client.get_rel_size(req).await
        })
        .await
    }

    /// Fetches an SLRU segment.
    pub async fn get_slru_segment(
        &self,
        req: page_api::GetSlruSegmentRequest,
    ) -> tonic::Result<page_api::GetSlruSegmentResponse> {
        self.with_retries("get_slru_segment", async || {
            // SLRU segments are only available on shard 0.
            let mut client = self.shards.get_zero().client().await?;
            client.get_slru_segment(req).await
        })
        .await
    }

    /// Runs the given closure with exponential backoff retries.
    async fn with_retries<T, F, O>(&self, name: &str, f: F) -> tonic::Result<T>
    where
        F: FnMut() -> O,
        O: Future<Output = tonic::Result<T>>,
    {
        /// TODO: tune retry parameters (retry forever?).
        /// TODO: add timeouts?
        const WARN_THRESHOLD: u32 = 1;
        const MAX_RETRIES: u32 = 10;

        fn is_permanent(err: &tonic::Status) -> bool {
            match err.code() {
                // Not really an error, but whatever. Don't retry.
                tonic::Code::Ok => true,

                // These codes are transient, so retry them.
                tonic::Code::Aborted => false,
                tonic::Code::Cancelled => false,
                tonic::Code::DeadlineExceeded => false, // maybe transient slowness
                tonic::Code::Internal => false,         // maybe transient failure
                tonic::Code::ResourceExhausted => false,
                tonic::Code::Unavailable => false,
                tonic::Code::Unknown => false, // may as well retry?

                // The following codes will like continue to fail, so don't retry.
                tonic::Code::AlreadyExists => true,
                tonic::Code::DataLoss => true,
                tonic::Code::FailedPrecondition => true,
                tonic::Code::InvalidArgument => true,
                tonic::Code::NotFound => true,
                tonic::Code::OutOfRange => true,
                tonic::Code::PermissionDenied => true,
                tonic::Code::Unimplemented => true,
                tonic::Code::Unauthenticated => true,
            }
        }

        // TODO: consider custom logic and logging here, using the caller's span for name.
        // TODO: cancellation? Could just drop the future.
        let cancel = CancellationToken::new();
        backoff::retry(f, is_permanent, WARN_THRESHOLD, MAX_RETRIES, name, &cancel)
            .await
            .expect("never cancelled (for now)")
    }
}

/// Tracks the tenant's shards.
struct Shards {
    /// The shard count.
    ///
    /// NB: this is 0 for unsharded tenants, following `ShardIndex::unsharded()` convention.
    count: ShardCount,
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
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        // TODO: support multiple shards.
        ensure!(shard_map.len() == 1, "multiple shards not supported");
        ensure!(
            shard_map.keys().next() == Some(&ShardIndex::unsharded()),
            "only unsharded tenant supported"
        );

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

        Ok(Self { count, map })
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

/// A single shard.
///
/// TODO: consider separate pools for normal and bulk traffic, with different settings.
struct Shard {
    /// Dedicated channel pool for this shard. Used by all clients/streams in this shard.
    _channel_pool: Arc<ChannelPool>,
    /// Unary gRPC client pool for this shard. Uses the shared channel pool.
    client_pool: Arc<ClientPool>,
    /// GetPage stream pool for this shard. Uses a dedicated client pool, but shares the channel
    /// pool with unary clients.
    stream_pool: Arc<StreamPool>,
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
        // Use a common channel pool for all clients, to multiplex unary and stream requests across
        // the same TCP connections. The channel pool is unbounded (but client pools are bounded).
        let channel_pool = ChannelPool::new(url)?;

        // Dedicated client pool for unary requests.
        let client_pool = ClientPool::new(
            channel_pool.clone(),
            tenant_id,
            timeline_id,
            shard_id,
            auth_token.clone(),
        );

        // Stream pool with dedicated client pool. If this shared a client pool with unary requests,
        // long-lived streams could fill up the client pool and starve out unary requests. It shares
        // the same underlying channel pool with unary clients though, which is unbounded.
        let stream_pool = StreamPool::new(ClientPool::new(
            channel_pool.clone(),
            tenant_id,
            timeline_id,
            shard_id,
            auth_token,
        ));

        Ok(Self {
            _channel_pool: channel_pool,
            client_pool,
            stream_pool,
        })
    }

    /// Returns a pooled client for this shard.
    async fn client(&self) -> tonic::Result<ClientGuard> {
        self.client_pool
            .get()
            .await
            .map_err(|err| tonic::Status::internal(format!("failed to get client: {err}")))
    }

    /// Returns a pooled stream for this shard.
    async fn stream(&self) -> StreamGuard {
        self.stream_pool.get().await
    }
}
