use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt};
use tokio::time::Instant;
use tracing::{error, info, instrument, warn};

use crate::pool::{ChannelPool, ClientGuard, ClientPool, StreamGuard, StreamPool};
use crate::split::GetPageSplitter;
use compute_api::spec::PageserverProtocol;
use pageserver_api::shard::ShardStripeSize;
use pageserver_page_api as page_api;
use utils::backoff::exponential_backoff_duration;
use utils::id::{TenantId, TimelineId};
use utils::shard::{ShardCount, ShardIndex, ShardNumber};

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
        Ok(Self { shards })
    }

    /// Returns whether a relation exists.
    #[instrument(skip_all, fields(rel=%req.rel, lsn=%req.read_lsn))]
    pub async fn check_rel_exists(
        &self,
        req: page_api::CheckRelExistsRequest,
    ) -> tonic::Result<page_api::CheckRelExistsResponse> {
        self.with_retries(async || {
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
        self.with_retries(async || {
            // Relation metadata is only available on shard 0.
            let mut client = self.shards.get_zero().client().await?;
            client.get_db_size(req).await
        })
        .await
    }

    /// Fetches pages. The `request_id` must be unique across all in-flight requests. Will
    /// automatically split requests that span multiple shards, and reassemble the responses.
    ///
    /// Unlike the `page_api::Client`, this client automatically converts `status_code` into
    /// `tonic::Status` errors. All responses will have `GetPageStatusCode::Ok`.
    #[instrument(skip_all, fields(
        req_id = %req.request_id,
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
            .with_retries(async || {
                let stream = self.shards.get(shard_id)?.stream().await;
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
        self.with_retries(async || {
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
        self.with_retries(async || {
            // SLRU segments are only available on shard 0.
            let mut client = self.shards.get_zero().client().await?;
            client.get_slru_segment(req).await
        })
        .await
    }

    /// Runs the given closure with retries (exponential backoff). Logs errors.
    async fn with_retries<T, F, O>(&self, mut f: F) -> tonic::Result<T>
    where
        F: FnMut() -> O,
        O: Future<Output = tonic::Result<T>>,
    {
        // TODO: tune these, and/or make them configurable. Should we retry forever?
        const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        const TOTAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        const BASE_BACKOFF: f64 = 0.1;
        const MAX_BACKOFF: f64 = 10.0;
        const LOG_SUCCESS: bool = false; // TODO: for debugging

        fn should_retry(code: tonic::Code) -> bool {
            match code {
                tonic::Code::Ok => panic!("unexpected Ok status code"),
                // These codes are transient, so retry them.
                tonic::Code::Aborted => true,
                tonic::Code::Cancelled => true,
                tonic::Code::DeadlineExceeded => true, // maybe transient slowness
                tonic::Code::Internal => true,         // maybe transient failure?
                tonic::Code::ResourceExhausted => true,
                tonic::Code::Unavailable => true,
                // The following codes will like continue to fail, so don't retry.
                tonic::Code::AlreadyExists => false,
                tonic::Code::DataLoss => false,
                tonic::Code::FailedPrecondition => false,
                tonic::Code::InvalidArgument => false,
                tonic::Code::NotFound => false,
                tonic::Code::OutOfRange => false,
                tonic::Code::PermissionDenied => false,
                tonic::Code::Unauthenticated => false,
                tonic::Code::Unimplemented => false,
                tonic::Code::Unknown => false,
            }
        }

        let started = Instant::now();
        let deadline = started + TOTAL_TIMEOUT;
        let mut last_error = None;
        let mut retries = 0;
        loop {
            // Set up a future to wait for the backoff (if any) and run the request with a timeout.
            let backoff = exponential_backoff_duration(retries, BASE_BACKOFF, MAX_BACKOFF);
            let backoff_and_try = async {
                tokio::time::sleep(backoff).await;
                let request_started = Instant::now();
                tokio::time::timeout(REQUEST_TIMEOUT, f())
                    .await
                    .map_err(|_| {
                        tonic::Status::deadline_exceeded(format!(
                            "request timed out after {:.3}s",
                            request_started.elapsed().as_secs_f64()
                        ))
                    })?
            };

            // Wait for the backoff and request, or bail out if the total timeout is exceeded.
            let result = tokio::select! {
                result = backoff_and_try => result,

                _ = tokio::time::sleep_until(deadline) => {
                    let last_error = last_error.unwrap_or_else(|| {
                        tonic::Status::deadline_exceeded(format!(
                            "request timed out after {:.3}s",
                            started.elapsed().as_secs_f64()
                        ))
                    });
                    error!(
                        "giving up after {:.3}s and {retries} retries, last error {:?}: {}",
                        started.elapsed().as_secs_f64(), last_error.code(), last_error.message(),
                    );
                    return Err(last_error);
                }
            };

            match result {
                Ok(result) => {
                    if retries > 0 || LOG_SUCCESS {
                        info!(
                            "request succeeded after {retries} retries in {:.3}s",
                            started.elapsed().as_secs_f64(),
                        );
                    }

                    return Ok(result);
                }

                Err(status) => {
                    let (code, message) = (status.code(), status.message());
                    let should_retry = should_retry(code);
                    let attempt = retries + 1;

                    if !should_retry {
                        // NB: include the attempt here too. This isn't necessarily the first
                        // attempt, because the error may change between attempts.
                        error!(
                            "request failed with {code:?}: {message}, not retrying (attempt {attempt})"
                        );
                        return Err(status);
                    }

                    warn!("request failed with {code:?}: {message}, retrying (attempt {attempt})");

                    retries += 1;
                    last_error = Some(status);
                }
            }
        }
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

/// A single shard.
///
/// TODO: consider separate pools for normal and bulk traffic, with different settings.
struct Shard {
    /// Dedicated channel pool for this shard. Shared by all clients/streams in this shard.
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
        // Sanity-check that the URL uses gRPC.
        if PageserverProtocol::from_connstring(&url)? != PageserverProtocol::Grpc {
            return Err(anyhow!("invalid shard URL {url}: must use gRPC"));
        }

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
