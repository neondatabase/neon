//! This module provides various Pageserver gRPC client resource pools.
//!
//! These pools are designed to reuse gRPC resources (connections, clients, and streams) across
//! multiple callers (i.e. Postgres backends). This avoids the resource cost and latency of creating
//! a dedicated TCP connection and server task for every Postgres backend.
//!
//! Each resource has its own, nested pool. The pools are custom-built for the properties of each
//! resource -- these are different enough that a generic pool isn't suitable.
//!
//! * ChannelPool: manages gRPC channels (TCP connections) to a single Pageserver. Multiple clients
//!   can acquire and use the same channel concurrently (via HTTP/2 stream multiplexing), up to a
//!   per-channel limit. Channels may be closed when they are no longer used by any clients.
//!
//! * ClientPool: manages gRPC clients for a single tenant shard. Each client acquires a (shared)
//!   channel from the ChannelPool for client's lifetime. A client can only be acquired by a single
//!   caller at a time, and is returned to the pool when dropped. Idle clients may be removed from
//!   the pool after some time, to free up the channel.
//!
//! * StreamPool: manages bidirectional gRPC GetPage streams. Each stream acquires a client from
//!   the ClientPool for the stream's lifetime. Internal streams are not exposed to callers;
//!   instead, callers submit individual GetPage requests to the pool and await a response.
//!   Internally, the pool will reuse or spin up a suitable stream for the request, possibly
//!   pipelining multiple requests from multiple callers on the same stream (up to some queue
//!   depth), and route the response back to the original caller. Idle streams may be removed from
//!   the pool after some time, to free up the client.

use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::StreamExt as _;
use scopeguard::defer;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tonic::transport::{Channel, Endpoint};
use tracing::warn;

use pageserver_page_api::{self as page_api, GetPageRequest, GetPageResponse};
use utils::id::{TenantId, TimelineId};
use utils::shard::ShardIndex;

// TODO: tune these constants, and consider making them configurable.

/// Max number of concurrent clients per channel.
///
/// TODO: consider separate limits for unary and streaming clients, so we don't fill up channels
/// with only streams.
const CLIENTS_PER_CHANNEL: usize = 16;

/// Maximum number of concurrent clients per `ClientPool`. This bounds the number of channels as
/// CLIENT_LIMIT / CLIENTS_PER_CHANNEL.
const CLIENT_LIMIT: usize = 64;

/// Max number of pipelined requests per gRPC GetPage stream.
const STREAM_QUEUE_DEPTH: usize = 2;

/// A gRPC channel pool, for a single Pageserver. A channel is shared by many clients (via HTTP/2
/// stream multiplexing), up to `CLIENTS_PER_CHANNEL`. The pool does not limit the number of
/// channels, and instead relies on `ClientPool` to limit the number of concurrent clients.
///
/// The pool is always wrapped in an outer `Arc`, to allow long-lived references from guards.
///
/// Tonic will automatically retry the underlying connection if it fails, so there is no need
/// to re-establish connections on errors.
///
/// TODO: reap idle channels.
/// TODO: consider adding a circuit breaker for errors and fail fast.
pub struct ChannelPool {
    /// Pageserver endpoint to connect to.
    endpoint: Endpoint,
    /// Open channels.
    channels: Mutex<BTreeMap<ChannelID, ChannelEntry>>,
}

type ChannelID = usize;

struct ChannelEntry {
    /// The gRPC channel (i.e. TCP connection). Shared by multiple clients.
    channel: Channel,
    /// Number of clients using this channel.
    clients: usize,
}

impl ChannelPool {
    /// Creates a new channel pool for the given Pageserver endpoint.
    pub fn new<E>(endpoint: E) -> anyhow::Result<Arc<Self>>
    where
        E: TryInto<Endpoint> + Send + Sync + 'static,
        <E as TryInto<Endpoint>>::Error: std::error::Error + Send + Sync,
    {
        Ok(Arc::new(Self {
            endpoint: endpoint.try_into()?,
            channels: Default::default(),
        }))
    }

    /// Acquires a gRPC channel for a client. Multiple clients may acquire the same channel.
    ///
    /// This never blocks (except for sync mutex acquisition). The channel is connected lazily on
    /// first use, and the `ChannelPool` does not have a channel limit.
    ///
    /// Callers should not clone the returned channel, and must hold onto the returned guard as long
    /// as the channel is in use. It is unfortunately not possible to enforce this: the Protobuf
    /// client requires an owned `Channel` and we don't have access to the channel's internal
    /// refcount.
    ///
    /// NB: this is not very performance-sensitive. It is only called when creating a new client,
    /// and clients are cached and reused by ClientPool. The total number of channels will also be
    /// small. O(n) performance is therefore okay.
    pub fn get(self: &Arc<Self>) -> anyhow::Result<ChannelGuard> {
        let mut channels = self.channels.lock().unwrap();

        // Try to find an existing channel with available capacity. We check entries in BTreeMap
        // order, to fill up the lower-ordered channels first. The ClientPool also uses clients with
        // lower-ordered channel IDs first. This will cluster clients in lower-ordered channels, and
        // free up higher-ordered channels such that they can be reaped.
        for (&id, entry) in channels.iter_mut() {
            assert!(entry.clients <= CLIENTS_PER_CHANNEL, "channel overflow");
            if entry.clients < CLIENTS_PER_CHANNEL {
                entry.clients += 1;
                return Ok(ChannelGuard {
                    pool: Arc::downgrade(self),
                    id,
                    channel: Some(entry.channel.clone()),
                });
            }
        }

        // Create a new channel. We connect lazily on the first use, such that we don't block here
        // and other clients can join onto the same channel while it's connecting.
        let channel = self.endpoint.connect_lazy();

        let id = channels.keys().last().copied().unwrap_or_default();
        let entry = ChannelEntry {
            channel: channel.clone(),
            clients: 1, // we're returning the guard below
        };
        channels.insert(id, entry);

        Ok(ChannelGuard {
            pool: Arc::downgrade(self),
            id,
            channel: Some(channel.clone()),
        })
    }
}

/// Tracks a channel acquired from the pool. The owned inner channel can be obtained with `take()`.
/// However, the caller must hold onto the guard as long as it's using the channel, and should not
/// clone it.
pub struct ChannelGuard {
    pool: Weak<ChannelPool>,
    id: ChannelID,
    channel: Option<Channel>,
}

impl ChannelGuard {
    /// Returns the inner channel. Panics if called more than once. The caller must hold onto the
    /// guard as long as the channel is in use, and should not clone it.
    pub fn take(&mut self) -> Channel {
        self.channel.take().expect("channel already taken")
    }
}

/// Returns the channel to the pool.
impl Drop for ChannelGuard {
    fn drop(&mut self) {
        let Some(pool) = self.pool.upgrade() else {
            return; // pool was dropped
        };
        let mut channels = pool.channels.lock().unwrap();
        let entry = channels.get_mut(&self.id).expect("unknown channel");
        assert!(entry.clients > 0, "channel underflow");
        entry.clients -= 1;
    }
}

/// A pool of gRPC clients for a single tenant shard. Each client acquires a channel from the inner
/// `ChannelPool`. A client is only acquired by a single caller at a time. The pool limits the total
/// number of concurrent clients to `CLIENT_LIMIT` via semaphore.
///
/// The pool is always wrapped in an outer `Arc`, to allow long-lived references from guards.
///
/// TODO: reap idle clients.
/// TODO: error handling (but channel will be reconnected automatically).
/// TODO: rate limiting.
pub struct ClientPool {
    /// Tenant ID.
    tenant_id: TenantId,
    /// Timeline ID.
    timeline_id: TimelineId,
    /// Shard ID.
    shard_id: ShardIndex,
    /// Authentication token, if any.
    auth_token: Option<String>,
    /// Channel pool to acquire channels from.
    channel_pool: Arc<ChannelPool>,
    /// Limits the max number of concurrent clients for this pool.
    limiter: Arc<Semaphore>,
    /// Idle pooled clients. Acquired clients are removed from here and returned on drop.
    ///
    /// The first client in the map will be acquired next. The map is sorted by client ID, which in
    /// turn is sorted by the channel ID, such that we prefer acquiring idle clients from
    /// lower-ordered channels. This allows us to free up and reap higher-numbered channels as idle
    /// clients are reaped.
    idle: Mutex<BTreeMap<ClientID, ClientEntry>>,
    /// Unique client ID generator.
    next_client_id: AtomicUsize,
}

type ClientID = (ChannelID, usize);

struct ClientEntry {
    client: page_api::Client,
    channel_guard: ChannelGuard,
}

impl ClientPool {
    /// Creates a new client pool for the given tenant shard. Channels are acquired from the given
    /// `ChannelPool`, which must point to a Pageserver that hosts the tenant shard.
    pub fn new(
        channel_pool: Arc<ChannelPool>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            tenant_id,
            timeline_id,
            shard_id,
            auth_token,
            channel_pool,
            idle: Mutex::default(),
            limiter: Arc::new(Semaphore::new(CLIENT_LIMIT)),
            next_client_id: AtomicUsize::default(),
        })
    }

    /// Gets a client from the pool, or creates a new one if necessary. Blocks if the pool is at
    /// `CLIENT_LIMIT`. The client is returned to the pool when the guard is dropped.
    ///
    /// This is moderately performance-sensitive. It is called for every unary request, but recall
    /// that these establish a new gRPC stream per request so it's already expensive. GetPage
    /// requests use the `StreamPool` instead.
    pub async fn get(self: &Arc<Self>) -> anyhow::Result<ClientGuard> {
        let permit = self
            .limiter
            .clone()
            .acquire_owned()
            .await
            .expect("never closed");

        // Fast path: acquire an idle client from the pool.
        if let Some((id, entry)) = self.idle.lock().unwrap().pop_first() {
            return Ok(ClientGuard {
                pool: Arc::downgrade(self),
                id,
                client: Some(entry.client),
                channel_guard: Some(entry.channel_guard),
                permit,
            });
        }

        // Slow path: construct a new client.
        let mut channel_guard = self.channel_pool.get()?;
        let client = page_api::Client::new(
            channel_guard.take(),
            self.tenant_id,
            self.timeline_id,
            self.shard_id,
            self.auth_token.clone(),
            None,
        )?;

        Ok(ClientGuard {
            pool: Arc::downgrade(self),
            id: (
                channel_guard.id,
                self.next_client_id.fetch_add(1, Ordering::Relaxed),
            ),
            client: Some(client),
            channel_guard: Some(channel_guard),
            permit,
        })
    }
}

/// A client acquired from the pool. The inner client can be accessed via derefs. The client is
/// returned to the pool when dropped.
pub struct ClientGuard {
    pool: Weak<ClientPool>,
    id: ClientID,
    client: Option<page_api::Client>,    // Some until dropped
    channel_guard: Option<ChannelGuard>, // Some until dropped
    permit: OwnedSemaphorePermit,
}

impl Deref for ClientGuard {
    type Target = page_api::Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("not dropped")
    }
}

impl DerefMut for ClientGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("not dropped")
    }
}

// Returns the client to the pool.
impl Drop for ClientGuard {
    fn drop(&mut self) {
        let Some(pool) = self.pool.upgrade() else {
            return; // pool was dropped
        };
        let entry = ClientEntry {
            client: self.client.take().expect("dropped once"),
            channel_guard: self.channel_guard.take().expect("dropped once"),
        };
        pool.idle.lock().unwrap().insert(self.id, entry);

        // The permit will be returned by its drop handler. Tag it here for visibility.
        _ = self.permit;
    }
}

/// A pool of bidirectional gRPC streams. Currently only used for GetPage streams. Each stream
/// acquires a client from the inner `ClientPool` for the stream's lifetime.
///
/// Individual streams are not exposed to callers -- instead, callers submit invididual requests to
/// the pool and await a response. Internally, requests are multiplexed across streams and channels.
///
/// TODO: reap idle streams.
/// TODO: error handling (but channel will be reconnected automatically).
/// TODO: rate limiting.
/// TODO: consider making this generic over request and response types; not currently needed.
pub struct StreamPool {
    /// The client pool to acquire clients from.
    client_pool: Arc<ClientPool>,
    /// All pooled streams.
    ///
    /// Incoming requests will be sent over an existing stream with available capacity, or a new
    /// stream is spun up and added to the pool. Each stream has an associated Tokio task that
    /// processes requests and responses.
    streams: Arc<Mutex<HashMap<StreamID, StreamEntry>>>,
    /// Limits the max number of concurrent requests (not streams).
    limiter: Semaphore,
    /// Stream ID generator.
    next_stream_id: AtomicUsize,
}

type StreamID = usize;
type RequestSender = Sender<(GetPageRequest, ResponseSender)>;
type RequestReceiver = Receiver<(GetPageRequest, ResponseSender)>;
type ResponseSender = oneshot::Sender<tonic::Result<GetPageResponse>>;

struct StreamEntry {
    /// Sends caller requests to the stream task. The stream task exits when this is dropped.
    sender: RequestSender,
    /// Number of in-flight requests on this stream. This is an atomic to allow decrementing it on
    /// completion without acquiring the `StreamPool::streams` lock.
    queue_depth: Arc<AtomicUsize>,
}

impl StreamPool {
    /// Creates a new stream pool, using the given client pool.
    pub fn new(client_pool: Arc<ClientPool>) -> Self {
        Self {
            client_pool,
            streams: Arc::default(),
            limiter: Semaphore::new(CLIENT_LIMIT * STREAM_QUEUE_DEPTH),
            next_stream_id: AtomicUsize::default(),
        }
    }

    /// Sends a request via the stream pool and awaits the response. Blocks if the pool is at
    /// capacity (i.e. `CLIENT_LIMIT * STREAM_QUEUE_DEPTH` requests in flight). The
    /// `GetPageRequest::request_id` must be unique across in-flight request.
    ///
    /// NB: errors are often returned as `GetPageResponse::status_code` instead of `tonic::Status`
    /// to avoid tearing down the stream for per-request errors. Callers must check this.
    ///
    /// This is very performance-sensitive, as it is on the GetPage hot path.
    ///
    /// TODO: this must do something more sophisticated for performance. We want:
    /// * Cheap, concurrent access in the common case where we can use a pooled stream.
    /// * Quick acquisition of pooled streams with available capacity.
    /// * Prefer streams that belong to lower-numbered channels, to reap idle channels.
    /// * Prefer filling up existing streams' queue depth before spinning up new streams.
    /// * Don't hold a lock while spinning up new streams.
    /// * Allow concurrent clients to join onto streams while they're spun up.
    /// * Allow spinning up multiple streams concurrently, but don't overshoot limits.
    ///
    /// For now, we just do something simple and functional, but very inefficient (linear scan).
    pub async fn send(&self, req: GetPageRequest) -> tonic::Result<GetPageResponse> {
        // Acquire a permit. For simplicity, we drop it when this method returns. This may exceed
        // the queue depth if a caller goes away while a request is in flight, but that's okay. We
        // do the same for queue depth tracking.
        let _permit = self.limiter.acquire().await.expect("never closed");

        // Acquire a stream sender. We increment and decrement the queue depth here instead of in
        // the stream task to ensure we don't exceed the queue depth limit.
        #[allow(clippy::await_holding_lock)] // TODO: Clippy doesn't understand drop()
        let (req_tx, queue_depth) = async {
            let mut streams = self.streams.lock().unwrap();

            // Try to find an existing stream with available capacity.
            for entry in streams.values() {
                assert!(
                    entry.queue_depth.load(Ordering::Relaxed) <= STREAM_QUEUE_DEPTH,
                    "stream overflow"
                );
                if entry
                    .queue_depth
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |queue_depth| {
                        // Increment the queue depth via compare-and-swap.
                        // TODO: review ordering.
                        (queue_depth < STREAM_QUEUE_DEPTH).then_some(queue_depth + 1)
                    })
                    .is_ok()
                {
                    return anyhow::Ok((entry.sender.clone(), entry.queue_depth.clone()));
                }
            }

            // No available stream, spin up a new one. We install the stream entry first and release
            // the lock, to allow other callers to join onto this stream and also create additional
            // streams concurrently when this fills up.
            let id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
            let queue_depth = Arc::new(AtomicUsize::new(1)); // account for this request
            let (req_tx, req_rx) = mpsc::channel(STREAM_QUEUE_DEPTH);
            let entry = StreamEntry {
                sender: req_tx.clone(),
                queue_depth: queue_depth.clone(),
            };
            streams.insert(id, entry);

            drop(streams); // drop lock before spinning up stream

            let client_pool = self.client_pool.clone();
            let streams = self.streams.clone();

            tokio::spawn(async move {
                if let Err(err) = Self::run_stream(client_pool, req_rx).await {
                    warn!("stream failed: {err}");
                }
                // Remove stream from pool on exit.
                let entry = streams.lock().unwrap().remove(&id);
                assert!(entry.is_some(), "unknown stream ID: {id}");
            });

            anyhow::Ok((req_tx, queue_depth))
        }
        .await
        .map_err(|err| tonic::Status::internal(err.to_string()))?;

        // Decrement the queue depth on return. This may prematurely decrement it if the caller goes
        // away while the request is in flight, but that's okay.
        defer!(
            let prev_queue_depth = queue_depth.fetch_sub(1, Ordering::SeqCst);
            assert!(prev_queue_depth > 0, "stream underflow");
        );

        // Send the request and wait for the response.
        let (resp_tx, resp_rx) = oneshot::channel();

        req_tx
            .send((req, resp_tx))
            .await
            .map_err(|_| tonic::Status::unavailable("stream closed"))?;

        resp_rx
            .await
            .map_err(|_| tonic::Status::unavailable("stream closed"))?
    }

    /// Runs a stream task. This acquires a client from the `ClientPool` and establishes a
    /// bidirectional GetPage stream, then forwards requests and responses between callers and the
    /// stream. It does not track or enforce queue depths, see `send()`.
    ///
    /// The task exits when the request channel is closed, or on a stream error. The caller is
    /// responsible for removing the stream from the pool on exit.
    async fn run_stream(
        client_pool: Arc<ClientPool>,
        mut caller_rx: RequestReceiver,
    ) -> anyhow::Result<()> {
        // Acquire a client from the pool and create a stream.
        let mut client = client_pool.get().await?;

        let (req_tx, req_rx) = mpsc::channel(STREAM_QUEUE_DEPTH);
        let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
        let mut resp_stream = client.get_pages(req_stream).await?;

        // Track caller response channels by request ID. If the task returns early, these response
        // channels will be dropped and the callers will receive an error.
        let mut callers = HashMap::with_capacity(STREAM_QUEUE_DEPTH);

        // Process requests and responses.
        loop {
            // NB: this can trip if the server doesn't respond to a request, so only debug_assert.
            debug_assert!(callers.len() <= STREAM_QUEUE_DEPTH, "stream overflow");

            tokio::select! {
                // Receive requests from callers and send them to the stream.
                req = caller_rx.recv() => {
                    // Shut down if request channel is closed.
                    let Some((req, resp_tx)) = req else {
                        return Ok(());
                    };

                    // Store the response channel by request ID.
                    if callers.contains_key(&req.request_id) {
                        // Error on request ID duplicates. Ignore callers that went away.
                        _ = resp_tx.send(Err(tonic::Status::invalid_argument(
                            format!("duplicate request ID: {}", req.request_id),
                        )));
                        continue;
                    }
                    callers.insert(req.request_id, resp_tx);

                    // Send the request on the stream. Bail out if the send fails.
                    req_tx.send(req).await.map_err(|_| {
                        tonic::Status::unavailable("stream closed")
                    })?;
                }

                // Receive responses from the stream and send them to callers.
                resp = resp_stream.next() => {
                    // Shut down if the stream is closed, and bail out on stream errors.
                    let Some(resp) = resp.transpose()? else {
                        return Ok(())
                    };

                    // Send the response to the caller. Ignore errors if the caller went away.
                    let Some(resp_tx) = callers.remove(&resp.request_id) else {
                        warn!("received response for unknown request ID: {}", resp.request_id);
                        continue;
                    };
                    _ = resp_tx.send(Ok(resp));
                }
            }
        }
    }
}
