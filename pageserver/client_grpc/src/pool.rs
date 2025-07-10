//! This module provides various Pageserver gRPC client resource pools.
//!
//! These pools are designed to reuse gRPC resources (connections, clients, and streams) across
//! multiple concurrent callers (i.e. Postgres backends). This avoids the resource cost and latency
//! of creating dedicated TCP connections and server tasks for every Postgres backend.
//!
//! Each resource has its own, nested pool. The pools are custom-built for the properties of each
//! resource -- they are different enough that a generic pool isn't suitable.
//!
//! * ChannelPool: manages gRPC channels (TCP connections) to a single Pageserver. Multiple clients
//!   can acquire and use the same channel concurrently (via HTTP/2 stream multiplexing), up to a
//!   per-channel client limit. Channels may be closed when they are no longer used by any clients.
//!
//! * ClientPool: manages gRPC clients for a single tenant shard. Each client acquires a (shared)
//!   channel from the ChannelPool for the client's lifetime. A client can only be acquired by a
//!   single caller at a time, and is returned to the pool when dropped. Idle clients may be removed
//!   from the pool after some time, to free up the channel.
//!
//! * StreamPool: manages bidirectional gRPC GetPage streams. Each stream acquires a client from the
//!   ClientPool for the stream's lifetime. Internal streams are not exposed to callers; instead, it
//!   returns a guard that can be used to send a single request, to properly enforce queue depth and
//!   route responses. Internally, the pool will reuse or spin up a suitable stream for the request,
//!   possibly pipelining multiple requests from multiple callers on the same stream (up to some
//!   queue depth). Idle streams may be removed from the pool after a while to free up the client.
//!
//! Each channel corresponds to one TCP connection. Each client unary request and each stream
//! corresponds to one HTTP/2 stream and server task.
//!
//! TODO: error handling (including custom error types).
//! TODO: observability.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZero;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use futures::StreamExt as _;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint};
use tracing::{error, warn};

use pageserver_page_api as page_api;
use utils::id::{TenantId, TimelineId};
use utils::shard::ShardIndex;

/// Reap channels/clients/streams that have been idle for this long.
///
/// TODO: this is per-pool. For nested pools, it can take up to 3x as long for a TCP connection to
/// be reaped. First, we must wait for an idle stream to be reaped, which marks its client as idle.
/// Then, we must wait for the idle client to be reaped, which marks its channel as idle. Then, we
/// must wait for the idle channel to be reaped. Is that a problem? Maybe not, we just have to
/// account for it when setting the reap threshold. Alternatively, we can immediately reap empty
/// channels, and/or stream pool clients.
const REAP_IDLE_THRESHOLD: Duration = match cfg!(any(test, feature = "testing")) {
    false => Duration::from_secs(180),
    true => Duration::from_secs(1), // exercise reaping in tests
};

/// Reap idle resources with this interval.
const REAP_IDLE_INTERVAL: Duration = match cfg!(any(test, feature = "testing")) {
    false => Duration::from_secs(10),
    true => Duration::from_secs(1), // exercise reaping in tests
};

/// A gRPC channel pool, for a single Pageserver. A channel is shared by many clients (via HTTP/2
/// stream multiplexing), up to `clients_per_channel` -- a new channel will be spun up beyond this.
/// The pool does not limit the number of channels, and instead relies on `ClientPool` or
/// `StreamPool` to limit the number of concurrent clients.
///
/// The pool is always wrapped in an outer `Arc`, to allow long-lived guards across tasks/threads.
///
/// TODO: consider prewarming a set of channels, to avoid initial connection latency.
/// TODO: consider adding a circuit breaker for errors and fail fast.
pub struct ChannelPool {
    /// Pageserver endpoint to connect to.
    endpoint: Endpoint,
    /// Max number of clients per channel. Beyond this, a new channel will be created.
    max_clients_per_channel: NonZero<usize>,
    /// Open channels.
    channels: Mutex<BTreeMap<ChannelID, ChannelEntry>>,
    /// Reaps idle channels.
    idle_reaper: Reaper,
    /// Channel ID generator.
    next_channel_id: AtomicUsize,
}

type ChannelID = usize;

struct ChannelEntry {
    /// The gRPC channel (i.e. TCP connection). Shared by multiple clients.
    channel: Channel,
    /// Number of clients using this channel.
    clients: usize,
    /// The channel has been idle (no clients) since this time. None if channel is in use.
    /// INVARIANT: Some if clients == 0, otherwise None.
    idle_since: Option<Instant>,
}

impl ChannelPool {
    /// Creates a new channel pool for the given Pageserver endpoint.
    pub fn new<E>(endpoint: E, max_clients_per_channel: NonZero<usize>) -> anyhow::Result<Arc<Self>>
    where
        E: TryInto<Endpoint> + Send + Sync + 'static,
        <E as TryInto<Endpoint>>::Error: std::error::Error + Send + Sync,
    {
        let pool = Arc::new(Self {
            endpoint: endpoint.try_into()?,
            max_clients_per_channel,
            channels: Mutex::default(),
            idle_reaper: Reaper::new(REAP_IDLE_THRESHOLD, REAP_IDLE_INTERVAL),
            next_channel_id: AtomicUsize::default(),
        });
        pool.idle_reaper.spawn(&pool);
        Ok(pool)
    }

    /// Acquires a gRPC channel for a client. Multiple clients may acquire the same channel.
    ///
    /// This never blocks (except for mutex acquisition). The channel is connected lazily on first
    /// use, and the `ChannelPool` does not have a channel limit. Channels will be re-established
    /// automatically on failure (TODO: verify).
    ///
    /// Callers should not clone the returned channel, and must hold onto the returned guard as long
    /// as the channel is in use. It is unfortunately not possible to enforce this: the Protobuf
    /// client requires an owned `Channel` and we don't have access to the channel's internal
    /// refcount.
    ///
    /// This is not performance-sensitive. It is only called when creating a new client, and clients
    /// are pooled and reused by `ClientPool`. The total number of channels will also be small. O(n)
    /// performance is therefore okay.
    pub fn get(self: &Arc<Self>) -> ChannelGuard {
        let mut channels = self.channels.lock().unwrap();

        // Try to find an existing channel with available capacity. We check entries in BTreeMap
        // order, to fill up the lower-ordered channels first. The ClientPool also prefers clients
        // with lower-ordered channel IDs first. This will cluster clients in lower-ordered
        // channels, and free up higher-ordered channels such that they can be reaped.
        for (&id, entry) in channels.iter_mut() {
            assert!(
                entry.clients <= self.max_clients_per_channel.get(),
                "channel overflow"
            );
            assert_eq!(
                entry.idle_since.is_some(),
                entry.clients == 0,
                "incorrect channel idle state"
            );
            if entry.clients < self.max_clients_per_channel.get() {
                entry.clients += 1;
                entry.idle_since = None;
                return ChannelGuard {
                    pool: Arc::downgrade(self),
                    id,
                    channel: Some(entry.channel.clone()),
                };
            }
        }

        // Create a new channel. We connect lazily on first use, such that we don't block here and
        // other clients can join onto the same channel while it's connecting.
        let channel = self.endpoint.connect_lazy();

        let id = self.next_channel_id.fetch_add(1, Ordering::Relaxed);
        let entry = ChannelEntry {
            channel: channel.clone(),
            clients: 1, // account for the guard below
            idle_since: None,
        };
        channels.insert(id, entry);

        ChannelGuard {
            pool: Arc::downgrade(self),
            id,
            channel: Some(channel),
        }
    }
}

impl Reapable for ChannelPool {
    /// Reaps channels that have been idle since before the cutoff.
    fn reap_idle(&self, cutoff: Instant) {
        self.channels.lock().unwrap().retain(|_, entry| {
            let Some(idle_since) = entry.idle_since else {
                assert_ne!(entry.clients, 0, "empty channel not marked idle");
                return true;
            };
            assert_eq!(entry.clients, 0, "idle channel has clients");
            idle_since >= cutoff
        })
    }
}

/// Tracks a channel acquired from the pool. The owned inner channel can be obtained with `take()`,
/// since the gRPC client requires an owned `Channel`.
pub struct ChannelGuard {
    pool: Weak<ChannelPool>,
    id: ChannelID,
    channel: Option<Channel>,
}

impl ChannelGuard {
    /// Returns the inner owned channel. Panics if called more than once. The caller must hold onto
    /// the guard as long as the channel is in use, and should not clone it.
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
        assert!(entry.idle_since.is_none(), "active channel marked idle");
        assert!(entry.clients > 0, "channel underflow");
        entry.clients -= 1;
        if entry.clients == 0 {
            entry.idle_since = Some(Instant::now()); // mark channel as idle
        }
    }
}

/// A pool of gRPC clients for a single tenant shard. Each client acquires a channel from the inner
/// `ChannelPool`. A client is only given out to single caller at a time. The pool limits the total
/// number of concurrent clients to `max_clients` via semaphore.
///
/// The pool is always wrapped in an outer `Arc`, to allow long-lived guards across tasks/threads.
pub struct ClientPool {
    /// Tenant ID.
    tenant_id: TenantId,
    /// Timeline ID.
    timeline_id: TimelineId,
    /// Shard ID.
    shard_id: ShardIndex,
    /// Authentication token, if any.
    auth_token: Option<String>,
    /// Compression to use.
    compression: Option<CompressionEncoding>,
    /// Channel pool to acquire channels from.
    channel_pool: Arc<ChannelPool>,
    /// Limits the max number of concurrent clients for this pool. None if the pool is unbounded.
    limiter: Option<Arc<Semaphore>>,
    /// Idle pooled clients. Acquired clients are removed from here and returned on drop.
    ///
    /// The first client in the map will be acquired next. The map is sorted by client ID, which in
    /// turn is sorted by its channel ID, such that we prefer acquiring idle clients from
    /// lower-ordered channels. This allows us to free up and reap higher-numbered channels as idle
    /// clients are reaped.
    idle: Mutex<BTreeMap<ClientID, ClientEntry>>,
    /// Reaps idle clients.
    idle_reaper: Reaper,
    /// Unique client ID generator.
    next_client_id: AtomicUsize,
}

type ClientID = (ChannelID, usize);

struct ClientEntry {
    /// The pooled gRPC client.
    client: page_api::Client,
    /// The channel guard for the channel used by the client.
    channel_guard: ChannelGuard,
    /// The client has been idle since this time. All clients in `ClientPool::idle` are idle by
    /// definition, so this is the time when it was added back to the pool.
    idle_since: Instant,
}

impl ClientPool {
    /// Creates a new client pool for the given tenant shard. Channels are acquired from the given
    /// `ChannelPool`, which must point to a Pageserver that hosts the tenant shard. Allows up to
    /// `max_clients` concurrent clients, or unbounded if None.
    pub fn new(
        channel_pool: Arc<ChannelPool>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
        compression: Option<CompressionEncoding>,
        max_clients: Option<NonZero<usize>>,
    ) -> Arc<Self> {
        let pool = Arc::new(Self {
            tenant_id,
            timeline_id,
            shard_id,
            auth_token,
            compression,
            channel_pool,
            idle: Mutex::default(),
            idle_reaper: Reaper::new(REAP_IDLE_THRESHOLD, REAP_IDLE_INTERVAL),
            limiter: max_clients.map(|max| Arc::new(Semaphore::new(max.get()))),
            next_client_id: AtomicUsize::default(),
        });
        pool.idle_reaper.spawn(&pool);
        pool
    }

    /// Gets a client from the pool, or creates a new one if necessary. Connections are established
    /// lazily and do not block, but this call can block if the pool is at `max_clients`. The client
    /// is returned to the pool when the guard is dropped.
    ///
    /// This is moderately performance-sensitive. It is called for every unary request, but these
    /// establish a new gRPC stream per request so they're already expensive. GetPage requests use
    /// the `StreamPool` instead.
    pub async fn get(self: &Arc<Self>) -> anyhow::Result<ClientGuard> {
        // Acquire a permit if the pool is bounded.
        let mut permit = None;
        if let Some(limiter) = self.limiter.clone() {
            permit = Some(limiter.acquire_owned().await.expect("never closed"));
        }

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
        let mut channel_guard = self.channel_pool.get();
        let client = page_api::Client::new(
            channel_guard.take(),
            self.tenant_id,
            self.timeline_id,
            self.shard_id,
            self.auth_token.clone(),
            self.compression,
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

impl Reapable for ClientPool {
    /// Reaps clients that have been idle since before the cutoff.
    fn reap_idle(&self, cutoff: Instant) {
        self.idle
            .lock()
            .unwrap()
            .retain(|_, entry| entry.idle_since >= cutoff)
    }
}

/// A client acquired from the pool. The inner client can be accessed via Deref. The client is
/// returned to the pool when dropped.
pub struct ClientGuard {
    pool: Weak<ClientPool>,
    id: ClientID,
    client: Option<page_api::Client>,     // Some until dropped
    channel_guard: Option<ChannelGuard>,  // Some until dropped
    permit: Option<OwnedSemaphorePermit>, // None if pool is unbounded
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

/// Returns the client to the pool.
impl Drop for ClientGuard {
    fn drop(&mut self) {
        let Some(pool) = self.pool.upgrade() else {
            return; // pool was dropped
        };

        let entry = ClientEntry {
            client: self.client.take().expect("dropped once"),
            channel_guard: self.channel_guard.take().expect("dropped once"),
            idle_since: Instant::now(),
        };
        pool.idle.lock().unwrap().insert(self.id, entry);

        _ = self.permit; // returned on drop, referenced for visibility
    }
}

/// A pool of bidirectional gRPC streams. Currently only used for GetPage streams. Each stream
/// acquires a client from the inner `ClientPool` for the stream's lifetime.
///
/// Individual streams are not exposed to callers -- instead, the returned guard can be used to send
/// a single request and await the response. Internally, requests are multiplexed across streams and
/// channels. This allows proper queue depth enforcement and response routing.
///
/// TODO: consider making this generic over request and response types; not currently needed.
pub struct StreamPool {
    /// The client pool to acquire clients from. Must be unbounded.
    client_pool: Arc<ClientPool>,
    /// All pooled streams.
    ///
    /// Incoming requests will be sent over an existing stream with available capacity. If all
    /// streams are full, a new one is spun up and added to the pool (up to `max_streams`). Each
    /// stream has an associated Tokio task that processes requests and responses.
    streams: Mutex<HashMap<StreamID, StreamEntry>>,
    /// The max number of concurrent streams, or None if unbounded.
    max_streams: Option<NonZero<usize>>,
    /// The max number of concurrent requests per stream.
    max_queue_depth: NonZero<usize>,
    /// Limits the max number of concurrent requests, given by `max_streams * max_queue_depth`.
    /// None if the pool is unbounded.
    limiter: Option<Arc<Semaphore>>,
    /// Reaps idle streams.
    idle_reaper: Reaper,
    /// Stream ID generator.
    next_stream_id: AtomicUsize,
}

type StreamID = usize;
type RequestSender = Sender<(page_api::GetPageRequest, ResponseSender)>;
type RequestReceiver = Receiver<(page_api::GetPageRequest, ResponseSender)>;
type ResponseSender = oneshot::Sender<tonic::Result<page_api::GetPageResponse>>;

struct StreamEntry {
    /// Sends caller requests to the stream task. The stream task exits when this is dropped.
    sender: RequestSender,
    /// Number of in-flight requests on this stream.
    queue_depth: usize,
    /// The time when this stream went idle (queue_depth == 0).
    /// INVARIANT: Some if queue_depth == 0, otherwise None.
    idle_since: Option<Instant>,
}

impl StreamPool {
    /// Creates a new stream pool, using the given client pool. It will send up to `max_queue_depth`
    /// concurrent requests on each stream, and use up to `max_streams` concurrent streams.
    ///
    /// The client pool must be unbounded. The stream pool will enforce its own limits, and because
    /// streams are long-lived they can cause persistent starvation if they exhaust the client pool.
    /// The stream pool should generally have its own dedicated client pool (but it can share a
    /// channel pool with others since these are always unbounded).
    pub fn new(
        client_pool: Arc<ClientPool>,
        max_streams: Option<NonZero<usize>>,
        max_queue_depth: NonZero<usize>,
    ) -> Arc<Self> {
        assert!(client_pool.limiter.is_none(), "bounded client pool");
        let pool = Arc::new(Self {
            client_pool,
            streams: Mutex::default(),
            limiter: max_streams.map(|max_streams| {
                Arc::new(Semaphore::new(max_streams.get() * max_queue_depth.get()))
            }),
            max_streams,
            max_queue_depth,
            idle_reaper: Reaper::new(REAP_IDLE_THRESHOLD, REAP_IDLE_INTERVAL),
            next_stream_id: AtomicUsize::default(),
        });
        pool.idle_reaper.spawn(&pool);
        pool
    }

    /// Acquires an available stream from the pool, or spins up a new stream async if all streams
    /// are full. Returns a guard that can be used to send a single request on the stream and await
    /// the response, with queue depth quota already acquired. Blocks if the pool is at capacity
    /// (i.e. `CLIENT_LIMIT * STREAM_QUEUE_DEPTH` requests in flight).
    ///
    /// This is very performance-sensitive, as it is on the GetPage hot path.
    ///
    /// TODO: this must do something more sophisticated for performance. We want:
    ///
    /// * Cheap, concurrent access in the common case where we can use a pooled stream.
    /// * Quick acquisition of pooled streams with available capacity.
    /// * Prefer streams that belong to lower-numbered channels, to reap idle channels.
    /// * Prefer filling up existing streams' queue depth before spinning up new streams.
    /// * Don't hold a lock while spinning up new streams.
    /// * Allow concurrent clients to join onto streams while they're spun up.
    /// * Allow spinning up multiple streams concurrently, but don't overshoot limits.
    ///
    /// For now, we just do something simple but inefficient (linear scan under mutex).
    pub async fn get(self: &Arc<Self>) -> StreamGuard {
        // Acquire a permit if the pool is bounded.
        let mut permit = None;
        if let Some(limiter) = self.limiter.clone() {
            permit = Some(limiter.acquire_owned().await.expect("never closed"));
        }
        let mut streams = self.streams.lock().unwrap();

        // Look for a pooled stream with available capacity.
        for (&id, entry) in streams.iter_mut() {
            assert!(
                entry.queue_depth <= self.max_queue_depth.get(),
                "stream queue overflow"
            );
            assert_eq!(
                entry.idle_since.is_some(),
                entry.queue_depth == 0,
                "incorrect stream idle state"
            );
            if entry.queue_depth < self.max_queue_depth.get() {
                entry.queue_depth += 1;
                entry.idle_since = None;
                return StreamGuard {
                    pool: Arc::downgrade(self),
                    id,
                    sender: entry.sender.clone(),
                    permit,
                };
            }
        }

        // No available stream, spin up a new one. We install the stream entry in the pool first and
        // return the guard, while spinning up the stream task async. This allows other callers to
        // join onto this stream and also create additional streams concurrently if this fills up.
        let id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let (req_tx, req_rx) = mpsc::channel(self.max_queue_depth.get());
        let entry = StreamEntry {
            sender: req_tx.clone(),
            queue_depth: 1, // reserve quota for this caller
            idle_since: None,
        };
        streams.insert(id, entry);

        if let Some(max_streams) = self.max_streams {
            assert!(streams.len() <= max_streams.get(), "stream overflow");
        };

        let client_pool = self.client_pool.clone();
        let pool = Arc::downgrade(self);

        tokio::spawn(async move {
            if let Err(err) = Self::run_stream(client_pool, req_rx).await {
                error!("stream failed: {err}");
            }
            // Remove stream from pool on exit. Weak reference to avoid holding the pool alive.
            if let Some(pool) = pool.upgrade() {
                let entry = pool.streams.lock().unwrap().remove(&id);
                assert!(entry.is_some(), "unknown stream ID: {id}");
            }
        });

        StreamGuard {
            pool: Arc::downgrade(self),
            id,
            sender: req_tx,
            permit,
        }
    }

    /// Runs a stream task. This acquires a client from the `ClientPool` and establishes a
    /// bidirectional GetPage stream, then forwards requests and responses between callers and the
    /// stream. It does not track or enforce queue depths -- that's done by `get()` since it must be
    /// atomic with pool stream acquisition.
    ///
    /// The task exits when the request channel is closed, or on a stream error. The caller is
    /// responsible for removing the stream from the pool on exit.
    async fn run_stream(
        client_pool: Arc<ClientPool>,
        mut caller_rx: RequestReceiver,
    ) -> anyhow::Result<()> {
        // Acquire a client from the pool and create a stream.
        let mut client = client_pool.get().await?;

        // NB: use an unbounded channel such that the stream send never blocks. Otherwise, we could
        // theoretically deadlock if both the client and server block on sends (since we're not
        // reading responses while sending). This is unlikely to happen due to gRPC/TCP buffers and
        // low queue depths, but it was seen to happen with the libpq protocol so better safe than
        // sorry. It should never buffer more than the queue depth anyway, but using an unbounded
        // channel guarantees that it will never block.
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let req_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(req_rx);
        let mut resp_stream = client.get_pages(req_stream).await?;

        // Track caller response channels by request ID. If the task returns early, these response
        // channels will be dropped and the waiting callers will receive an error.
        let mut callers = HashMap::new();

        // Process requests and responses.
        loop {
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

                    // Send the request on the stream. Bail out if the stream is closed.
                    req_tx.send(req).map_err(|_| {
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

impl Reapable for StreamPool {
    /// Reaps streams that have been idle since before the cutoff.
    fn reap_idle(&self, cutoff: Instant) {
        self.streams.lock().unwrap().retain(|_, entry| {
            let Some(idle_since) = entry.idle_since else {
                assert_ne!(entry.queue_depth, 0, "empty stream not marked idle");
                return true;
            };
            assert_eq!(entry.queue_depth, 0, "idle stream has requests");
            idle_since >= cutoff
        });
    }
}

/// A pooled stream reference. Can be used to send a single request, to properly enforce queue
/// depth. Queue depth is already reserved and will be returned on drop.
pub struct StreamGuard {
    pool: Weak<StreamPool>,
    id: StreamID,
    sender: RequestSender,
    permit: Option<OwnedSemaphorePermit>, // None if pool is unbounded
}

impl StreamGuard {
    /// Sends a request on the stream and awaits the response. Consumes the guard, since it's only
    /// valid for a single request (to enforce queue depth). This also drops the guard on return and
    /// returns the queue depth quota to the pool.
    ///
    /// The `GetPageRequest::request_id` must be unique across in-flight requests.
    ///
    /// NB: errors are often returned as `GetPageResponse::status_code` instead of `tonic::Status`
    /// to avoid tearing down the stream for per-request errors. Callers must check this.
    pub async fn send(
        self,
        req: page_api::GetPageRequest,
    ) -> tonic::Result<page_api::GetPageResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.sender
            .send((req, resp_tx))
            .await
            .map_err(|_| tonic::Status::unavailable("stream closed"))?;

        resp_rx
            .await
            .map_err(|_| tonic::Status::unavailable("stream closed"))?
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let Some(pool) = self.pool.upgrade() else {
            return; // pool was dropped
        };

        // Release the queue depth reservation on drop. This can prematurely decrement it if dropped
        // before the response is received, but that's okay.
        let mut streams = pool.streams.lock().unwrap();
        let entry = streams.get_mut(&self.id).expect("unknown stream");
        assert!(entry.idle_since.is_none(), "active stream marked idle");
        assert!(entry.queue_depth > 0, "stream queue underflow");
        entry.queue_depth -= 1;
        if entry.queue_depth == 0 {
            entry.idle_since = Some(Instant::now()); // mark stream as idle
        }

        _ = self.permit; // returned on drop, referenced for visibility
    }
}

/// Periodically reaps idle resources from a pool.
struct Reaper {
    /// The task check interval.
    interval: Duration,
    /// The threshold for reaping idle resources.
    threshold: Duration,
    /// Cancels the reaper task. Cancelled when the reaper is dropped.
    cancel: CancellationToken,
}

impl Reaper {
    /// Creates a new reaper.
    pub fn new(threshold: Duration, interval: Duration) -> Self {
        Self {
            cancel: CancellationToken::new(),
            threshold,
            interval,
        }
    }

    /// Spawns a task to periodically reap idle resources from the given task pool. The task is
    /// cancelled when the reaper is dropped.
    pub fn spawn(&self, pool: &Arc<impl Reapable>) {
        // NB: hold a weak pool reference, otherwise the task will prevent dropping the pool.
        let pool = Arc::downgrade(pool);
        let cancel = self.cancel.clone();
        let (interval, threshold) = (self.interval, self.threshold);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        let Some(pool) = pool.upgrade() else {
                            return; // pool was dropped
                        };
                        pool.reap_idle(Instant::now() - threshold);
                    }

                    _ = cancel.cancelled() => return,
                }
            }
        });
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        self.cancel.cancel(); // cancel reaper task
    }
}

/// A reapable resource pool.
trait Reapable: Send + Sync + 'static {
    /// Reaps resources that have been idle since before the given cutoff.
    fn reap_idle(&self, cutoff: Instant);
}
