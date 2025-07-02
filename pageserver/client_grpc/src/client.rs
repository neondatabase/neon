use std::collections::HashMap;
use std::sync::Arc;

use anyhow::ensure;
use pageserver_page_api as page_api;
use tokio_util::sync::CancellationToken;
use utils::backoff;
use utils::id::{TenantId, TimelineId};
use utils::shard::ShardIndex;

use crate::pool::{ChannelPool, ClientGuard, ClientPool, StreamGuard, StreamPool};

/// A rich Pageserver gRPC client for a single tenant timeline. This client is more capable than the
/// basic `page_api::Client` gRPC client, and supports:
///
/// * Sharded tenants across multiple Pageservers.
/// * Pooling of connections, clients, and streams for efficient resource use.
/// * Concurrent use by many callers.
/// * Internal handling of GetPage bidirectional streams.
/// * Automatic retries.
///
/// TODO: this client does not support base backups or LSN leases, as these are only used by
/// compute_ctl. Consider adding this.
///
/// TODO: use a proper error type.
pub struct PageserverClient {
    /// Resource pools per shard.
    pools: HashMap<ShardIndex, ShardPools>,
}

impl PageserverClient {
    /// Creates a new Pageserver client.
    pub fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_map: HashMap<ShardIndex, String>,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        // TODO: support multiple shards.
        ensure!(shard_map.len() == 1, "multiple shard not supported");
        ensure!(
            shard_map.keys().next() == Some(&ShardIndex::unsharded()),
            "only unsharded tenant supported"
        );

        let mut pools = HashMap::new();
        for (shard_id, url) in shard_map {
            let shard_pools =
                ShardPools::new(url, tenant_id, timeline_id, shard_id, auth_token.clone())?;
            pools.insert(shard_id, shard_pools);
        }

        Ok(Self { pools })
    }

    /// Returns whether a relation exists.
    pub async fn check_rel_exists(
        &self,
        req: page_api::CheckRelExistsRequest,
    ) -> tonic::Result<page_api::CheckRelExistsResponse> {
        // Relation metadata is only available on shard 0.
        let shard_id = self.shard_zero();

        self.with_retries("check_rel_exists", async || {
            let mut client = self.get_shard_client(shard_id).await?;
            client.check_rel_exists(req).await
        })
        .await
    }

    /// Returns the total size of a database, as # of bytes.
    pub async fn get_db_size(
        &self,
        req: page_api::GetDbSizeRequest,
    ) -> tonic::Result<page_api::GetDbSizeResponse> {
        // Relation metadata is only available on shard 0.
        let shard_id = self.shard_zero();

        self.with_retries("get_db_size", async || {
            let mut client = self.get_shard_client(shard_id).await?;
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
            let stream = self.get_shard_stream(shard_id).await?;
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
        // Relation metadata is only available on shard 0.
        let shard_id = self.shard_zero();

        self.with_retries("get_rel_size", async || {
            let mut client = self.get_shard_client(shard_id).await?;
            client.get_rel_size(req).await
        })
        .await
    }

    /// Fetches an SLRU segment.
    pub async fn get_slru_segment(
        &self,
        req: page_api::GetSlruSegmentRequest,
    ) -> tonic::Result<page_api::GetSlruSegmentResponse> {
        // SLRU segments are only available on shard 0.
        let shard_id = self.shard_zero();

        self.with_retries("get_slru_segment", async || {
            let mut client = self.get_shard_client(shard_id).await?;
            client.get_slru_segment(req).await
        })
        .await
    }

    /// Returns a pooled `page_api::Client` for the given shard.
    async fn get_shard_client(&self, shard_id: ShardIndex) -> tonic::Result<ClientGuard> {
        self.pools
            .get(&shard_id)
            .ok_or_else(|| tonic::Status::not_found(format!("unknown shard {shard_id}")))?
            .clients
            .get()
            .await
            .map_err(|err| tonic::Status::internal(format!("failed to acquire client: {err}")))
    }

    /// Returns a pooled stream for the given shard.
    #[allow(clippy::result_large_err)] // TODO: revisit
    async fn get_shard_stream(&self, shard_id: ShardIndex) -> tonic::Result<StreamGuard> {
        Ok(self
            .pools
            .get(&shard_id)
            .ok_or_else(|| tonic::Status::not_found(format!("unknown shard {shard_id}")))?
            .streams
            .get()
            .await)
    }

    /// Returns the shard index for shard 0.
    fn shard_zero(&self) -> ShardIndex {
        // TODO: support multiple shards.
        ShardIndex::unsharded()
    }

    /// Runs the given closure with exponential backoff retries.
    async fn with_retries<T, F, O>(&self, name: &str, f: F) -> tonic::Result<T>
    where
        F: FnMut() -> O,
        O: Future<Output = tonic::Result<T>>,
    {
        /// TODO: tune retry parameters (retry forever?).
        /// TODO: add timeouts.
        const WARN_THRESHOLD: u32 = 1;
        const MAX_RETRIES: u32 = 10;
        // TODO: cancellation.
        let cancel = CancellationToken::new();

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
                tonic::Code::Unknown => false, // may as well retry
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

        backoff::retry(f, is_permanent, WARN_THRESHOLD, MAX_RETRIES, name, &cancel)
            .await
            .expect("never cancelled (for now)")
    }
}

/// Resource pools for a single shard.
///
/// TODO: consider separate pools for normal and bulk traffic, with different settings.
struct ShardPools {
    /// Manages unary gRPC clients for this shard.
    clients: Arc<ClientPool>,
    /// Manages gRPC GetPage streams for this shard. Uses a dedicated client pool, but shares the
    /// channel pool with unary clients.
    streams: Arc<StreamPool>,
}

impl ShardPools {
    /// Creates a new set of resource pools for the given shard.
    pub fn new(
        url: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        // Use a common channel pool for all clients, to multiplex unary and stream requests across
        // the same TCP connections. The channel pool is unbounded (client pools are bounded).
        let channels = ChannelPool::new(url)?;

        // Dedicated client pool for unary requests.
        let clients = ClientPool::new(
            channels.clone(),
            tenant_id,
            timeline_id,
            shard_id,
            auth_token.clone(),
        );

        // Dedicated client pool for streams. If this shared a client pool with unary requests,
        // long-lived streams could fill up the client pool and starve out unary requests. It
        // shares the same underlying channel pool with unary clients though.
        let stream_clients =
            ClientPool::new(channels, tenant_id, timeline_id, shard_id, auth_token);
        let streams = StreamPool::new(stream_clients);

        Ok(Self { clients, streams })
    }
}
