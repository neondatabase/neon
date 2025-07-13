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
//!   per-channel client limit.
//!
//! * ClientPool: manages gRPC clients for a single tenant shard. Each client acquires a (shared)
//!   channel from the ChannelPool for the client's lifetime. A client can only be acquired by a
//!   single caller at a time, and is returned to the pool when dropped.
//!
//! * StreamPool: manages bidirectional gRPC GetPage streams. Each stream acquires a client from the
//!   ClientPool for the stream's lifetime. A stream can only be acquired by a single caller at a
//!   time, and is returned to the pool when dropped. The stream only supports sending a single,
//!   synchronous request at a time, and does not support pipelining multiple requests from
//!   different callers onto the same stream -- instead, we scale out concurrent streams to improve
//!   throughput. There are many reasons for this design choice:
//!
//!     * It (mostly) eliminates head-of-line blocking. A single stream is processed sequentially by
//!       a single server task, which may block e.g. on layer downloads, LSN waits, etc.
//!
//!     * Cancellation becomes trivial, by closing the stream. Otherwise, if a caller goes away
//!       (e.g. because of a timeout), the request would still be processed by the server and block
//!       requests behind it in the stream. It might even block its own timeout retry.
//!
//!     * Individual callers can use client-side batching for pipelining.
//!
//!     * Stream scheduling becomes significantly simpler and cheaper.
//!
//!     * Idle streams are cheap. Benchmarks show that an idle GetPage stream takes up about 26 KB
//!       per stream (2.5 GB for 100,000 streams), so we can afford to scale out.
//!
//! Idle resources are removed from the pools periodically, to avoid holding onto server resources.
//!
//! Each channel corresponds to one TCP connection. Each client unary request and each stream
//! corresponds to one HTTP/2 stream and server task.
//!
//! TODO: error handling (including custom error types).
//! TODO: observability.

use std::collections::BTreeMap;
use std::num::NonZero;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_stream::wrappers::WatchStream;
use tokio_util::sync::CancellationToken;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint};

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
    /// lower-ordered channels. This allows us to free up and reap higher-ordered channels.
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
    pub async fn get(self: &Arc<Self>) -> tonic::Result<ClientGuard> {
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

        // Construct a new client.
        let mut channel_guard = self.channel_pool.get();
        let client = page_api::Client::new(
            channel_guard.take(),
            self.tenant_id,
            self.timeline_id,
            self.shard_id,
            self.auth_token.clone(),
            self.compression,
        )
        .map_err(|err| tonic::Status::internal(format!("failed to create client: {err}")))?;

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
/// Individual streams only send a single request at a time, and do not pipeline multiple callers
/// onto the same stream. Instead, we scale out the number of concurrent streams. This is primarily
/// to eliminate head-of-line blocking. See the module documentation for more details.
///
/// TODO: consider making this generic over request and response types; not currently needed.
pub struct StreamPool {
    /// The client pool to acquire clients from. Must be unbounded.
    client_pool: Arc<ClientPool>,
    /// Idle pooled streams. Acquired streams are removed from here and returned on drop.
    ///
    /// The first stream in the map will be acquired next. The map is sorted by stream ID, which is
    /// equivalent to the client ID and in turn sorted by its channel ID. This way we prefer
    /// acquiring idle streams from lower-ordered channels, which allows us to free up and reap
    /// higher-ordered channels.
    idle: Mutex<BTreeMap<StreamID, StreamEntry>>,
    /// Limits the max number of concurrent streams. None if the pool is unbounded.
    limiter: Option<Arc<Semaphore>>,
    /// Reaps idle streams.
    idle_reaper: Reaper,
}

/// The stream ID. Reuses the inner client ID.
type StreamID = ClientID;

/// A pooled stream.
struct StreamEntry {
    /// The bidirectional stream.
    stream: BiStream,
    /// The time when this stream was last used, i.e. when it was put back into `StreamPool::idle`.
    idle_since: Instant,
}

/// A bidirectional GetPage stream and its client. Can send requests and receive responses.
struct BiStream {
    /// The owning client. Holds onto the channel slot while the stream is alive.
    client: ClientGuard,
    /// Stream for sending requests. Uses a watch channel, so it can only send a single request at a
    /// time, and the caller must await the response before sending another request. This is
    /// enforced by `StreamGuard::send`.
    sender: watch::Sender<page_api::GetPageRequest>,
    /// Stream for receiving responses.
    /// TODO: consider returning a concrete type from `Client::get_pages`.
    receiver: Pin<Box<dyn Stream<Item = tonic::Result<page_api::GetPageResponse>> + Send>>,
}

impl StreamPool {
    /// Creates a new stream pool, using the given client pool. It will use up to `max_streams`
    /// concurrent streams.
    ///
    /// The client pool must be unbounded. The stream pool will enforce its own limits, and because
    /// streams are long-lived they can cause persistent starvation if they exhaust the client pool.
    /// The stream pool should generally have its own dedicated client pool (but it can share a
    /// channel pool with others since these are always unbounded).
    pub fn new(client_pool: Arc<ClientPool>, max_streams: Option<NonZero<usize>>) -> Arc<Self> {
        assert!(client_pool.limiter.is_none(), "bounded client pool");
        let pool = Arc::new(Self {
            client_pool,
            idle: Mutex::default(),
            limiter: max_streams.map(|max_streams| Arc::new(Semaphore::new(max_streams.get()))),
            idle_reaper: Reaper::new(REAP_IDLE_THRESHOLD, REAP_IDLE_INTERVAL),
        });
        pool.idle_reaper.spawn(&pool);
        pool
    }

    /// Acquires an available stream from the pool, or spins up a new stream if all streams are
    /// full. Returns a guard that can be used to send requests and await the responses. Blocks if
    /// the pool is full.
    ///
    /// This is very performance-sensitive, as it is on the GetPage hot path.
    ///
    /// TODO: is a Mutex<BTreeMap> performant enough? Will it become too contended? We can't
    /// trivially use e.g. DashMap or sharding, because we want to pop lower-ordered streams first
    /// to free up higher-ordered channels.
    pub async fn get(self: &Arc<Self>) -> tonic::Result<StreamGuard> {
        // Acquire a permit if the pool is bounded.
        let mut permit = None;
        if let Some(limiter) = self.limiter.clone() {
            permit = Some(limiter.acquire_owned().await.expect("never closed"));
        }

        // Fast path: acquire an idle stream from the pool.
        if let Some((_, entry)) = self.idle.lock().unwrap().pop_first() {
            return Ok(StreamGuard {
                pool: Arc::downgrade(self),
                stream: Some(entry.stream),
                active: false,
                permit,
            });
        }

        // Spin up a new stream. Uses a watch channel to send a single request at a time, since
        // `StreamGuard::send` enforces this anyway and it avoids unnecessary channel overhead.
        let mut client = self.client_pool.get().await?;

        let (req_tx, req_rx) = watch::channel(page_api::GetPageRequest::default());
        let req_stream = WatchStream::from_changes(req_rx);
        let resp_stream = client.get_pages(req_stream).await?;

        Ok(StreamGuard {
            pool: Arc::downgrade(self),
            stream: Some(BiStream {
                client,
                sender: req_tx,
                receiver: Box::pin(resp_stream),
            }),
            active: false,
            permit,
        })
    }
}

impl Reapable for StreamPool {
    /// Reaps streams that have been idle since before the cutoff.
    fn reap_idle(&self, cutoff: Instant) {
        self.idle
            .lock()
            .unwrap()
            .retain(|_, entry| entry.idle_since >= cutoff);
    }
}

/// A stream acquired from the pool. Returned to the pool when dropped, unless there are still
/// in-flight requests on the stream, or the stream failed.
pub struct StreamGuard {
    pool: Weak<StreamPool>,
    stream: Option<BiStream>,             // Some until dropped
    active: bool,                         // not returned to pool if true
    permit: Option<OwnedSemaphorePermit>, // None if pool is unbounded
}

impl StreamGuard {
    /// Sends a request on the stream and awaits the response. If the future is dropped before it
    /// resolves (e.g. due to a timeout or cancellation), the stream will be closed to cancel the
    /// request and is not returned to the pool. The same is true if the stream errors, in which
    /// case the caller can't send further requests on the stream.
    ///
    /// We only support sending a single request at a time, to eliminate head-of-line blocking. See
    /// module documentation for details.
    ///
    /// NB: errors are often returned as `GetPageResponse::status_code` instead of `tonic::Status`
    /// to avoid tearing down the stream for per-request errors. Callers must check this.
    pub async fn send(
        &mut self,
        req: page_api::GetPageRequest,
    ) -> tonic::Result<page_api::GetPageResponse> {
        let req_id = req.request_id;
        let stream = self.stream.as_mut().expect("not dropped");

        // Mark the stream as active. We only allow one request at a time, to avoid HoL-blocking.
        // We also don't allow reuse of the stream after an error.
        //
        // NB: this uses a watch channel, so it's unsafe to change this code to pipeline requests.
        if self.active {
            return Err(tonic::Status::internal("stream already active"));
        }
        self.active = true;

        // Send the request and receive the response. If the stream errors for whatever reason, we
        // don't reset `active` such that the stream won't be returned to the pool.
        stream
            .sender
            .send(req)
            .map_err(|_| tonic::Status::unavailable("stream closed"))?;

        let resp = stream
            .receiver
            .next()
            .await
            .ok_or_else(|| tonic::Status::unavailable("stream closed"))??;

        if resp.request_id != req_id {
            return Err(tonic::Status::internal(format!(
                "response ID {} does not match request ID {}",
                resp.request_id, req_id
            )));
        }

        // Success, mark the stream as inactive.
        self.active = false;

        Ok(resp)
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let Some(pool) = self.pool.upgrade() else {
            return; // pool was dropped
        };

        // If the stream is still active, we can't return it to the pool. The next caller could be
        // head-of-line blocked by an in-flight request, receive a stale response, or the stream may
        // have failed.
        if self.active {
            return;
        }

        // Place the idle stream back into the pool.
        let entry = StreamEntry {
            stream: self.stream.take().expect("dropped once"),
            idle_since: Instant::now(),
        };
        pool.idle
            .lock()
            .unwrap()
            .insert(entry.stream.client.id, entry);

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
