use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::StreamExt;
use scopeguard::defer;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::{Channel, Endpoint};

use pageserver_page_api::{self as page_api, GetPageRequest, GetPageResponse};
use tracing::warn;
use utils::id::{TenantId, TimelineId};
use utils::shard::ShardIndex;

/// A gRPC channel pool. A channel is shared by many clients, using HTTP/2 stream multiplexing.
/// This pool allows an unlimited number of channels. Concurrency is limited by ClientPool. It is
/// not performance-critical, because clients (and thus channels) will be reused by ClientPool.
///
/// This doesn't use the `Pool` type, because it's designed for exclusive access, while a channel is
/// shared by many clients. Furthermore, we can't build a generic ArcPool for shared items, because
/// Protobuf clients require an owned Channel (not an Arc<Channel>), and we don't have access to the
/// Channel refcount.
struct ChannelPool {
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
    /// Max number of concurrent clients per channel.
    ///
    /// TODO: tune this.
    /// TODO: consider having separate limits for unary and streaming clients. This way, a channel
    /// that's full of streaming requests also has room for a few unary requests.
    const CLIENTS_PER_CHANNEL: usize = 16;

    /// Creates a new channel pool for the given Pageserver URL.
    pub fn new(url: String) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            endpoint: Endpoint::from_shared(url)?,
            channels: Default::default(),
        }))
    }

    /// Acquires a new gRPC channel.
    ///
    /// NB: this is not particularly performance-sensitive. It is called rarely since clients are
    /// cached and reused by ClientPool, and the number of channels will be small. O(n) performance
    /// is therefore okay.
    pub fn get(self: Arc<Self>) -> anyhow::Result<ChannelGuard> {
        let mut channels = self.channels.lock().unwrap();

        // Find an existing channel with available capacity. We check entries in BTreeMap order,
        // such that we fill up the earliest channels first. The ClientPool also uses lower-ordered
        // channels first. This allows us to reap later channels as they become idle.
        for (&id, entry) in channels.iter_mut() {
            if entry.clients < Self::CLIENTS_PER_CHANNEL {
                entry.clients += 1;
                return Ok(ChannelGuard {
                    pool: Arc::downgrade(&self),
                    id,
                    channel: Some(entry.channel.clone()),
                });
            }
        }

        // Create a new channel. We connect lazily, such that we don't block and other clients can
        // join onto the same channel.
        let id = channels.keys().last().copied().unwrap_or_default();
        let channel = self.endpoint.connect_lazy();
        let guard = ChannelGuard {
            pool: Arc::downgrade(&self),
            id,
            channel: Some(channel.clone()),
        };
        let entry = ChannelEntry {
            channel,
            clients: 1,
        };
        channels.insert(id, entry);

        Ok(guard)
    }
}

struct ChannelGuard {
    pool: Weak<ChannelPool>,
    id: ChannelID,
    channel: Option<Channel>,
}

impl ChannelGuard {
    /// Returns the inner channel. Can only be called once. The caller must hold onto the guard as
    /// long as the channel is in use, and should not clone it.
    ///
    /// Unfortunately, we can't enforce that the guard outlives the channel reference, because a
    /// Protobuf client requires an owned `Channel` and we don't have access to the channel's
    /// internal refcount either. We could if the client took an `Arc<Channel>`.
    pub fn take(&mut self) -> Channel {
        self.channel.take().expect("channel")
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
        assert!(entry.clients > 0, "channel clients underflow");
        entry.clients -= 1;
    }
}

/// A pool of gRPC clients.
pub struct ClientPool {
    /// Tenant ID.
    tenant_id: TenantId,
    /// Timeline ID.
    timeline_id: TimelineId,
    /// Shard ID.
    shard_id: ShardIndex,
    /// Authentication token, if any.
    auth_token: Option<String>,
    /// Channel pool.
    channels: Arc<ChannelPool>,
    /// Limits the max number of concurrent clients.
    limiter: Arc<Semaphore>,
    /// Idle clients in the pool. This is sorted by channel ID and client ID, such that we use idle
    /// clients from the lower-numbered channels first. This allows us to reap the higher-numbered
    /// channels as they become idle.
    idle: Mutex<BTreeMap<ClientKey, ClientEntry>>,
    /// Unique client ID generator.
    next_client_id: AtomicUsize,
}

type ClientID = usize;
type ClientKey = (ChannelID, ClientID);
struct ClientEntry {
    client: page_api::Client,
    channel_guard: ChannelGuard,
}

impl ClientPool {
    const CLIENT_LIMIT: usize = 64; // TODO: make this configurable

    /// Creates a new client pool for the given Pageserver and tenant shard.
    pub fn new(
        url: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            tenant_id,
            timeline_id,
            shard_id,
            auth_token,
            channels: ChannelPool::new(url)?,
            idle: Mutex::default(),
            limiter: Arc::new(Semaphore::new(Self::CLIENT_LIMIT)),
            next_client_id: AtomicUsize::default(),
        })
    }

    /// Gets a client from the pool, or creates a new one if necessary. The client is returned to
    /// the pool when the guard is dropped.
    pub async fn get(self: Arc<Self>) -> anyhow::Result<ClientGuard> {
        let permit = self
            .limiter
            .clone()
            .acquire_owned()
            .await
            .expect("never closed");
        let mut idle = self.idle.lock().unwrap();

        // Fast path: acquire an idle client from the pool.
        if let Some(((_, id), entry)) = idle.pop_first() {
            return Ok(ClientGuard {
                pool: Arc::downgrade(&self),
                id,
                client: Some(entry.client),
                channel_guard: Some(entry.channel_guard),
                permit,
            });
        }

        // Slow path: construct a new client.
        let mut channel_guard = self.channels.clone().get()?; // never blocks (lazy connection)
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);

        let client = page_api::Client::new(
            channel_guard.take(),
            self.tenant_id,
            self.timeline_id,
            self.shard_id,
            self.auth_token.clone(),
            None,
        )?;

        Ok(ClientGuard {
            pool: Arc::downgrade(&self),
            id,
            client: Some(client),
            channel_guard: Some(channel_guard),
            permit,
        })
    }
}

pub struct ClientGuard {
    pool: Weak<ClientPool>,
    id: ClientID,
    client: Option<page_api::Client>,
    channel_guard: Option<ChannelGuard>,
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
        let mut idle = pool.idle.lock().unwrap();
        let client = self.client.take().expect("dropped once");
        let channel_guard = self.channel_guard.take().expect("dropped once");
        let channel_id = channel_guard.id;
        let entry = ClientEntry {
            client,
            channel_guard,
        };
        idle.insert((channel_id, self.id), entry);

        // The permit will be returned by its drop handler. Tag it here for visibility.
        _ = self.permit;
    }
}

/// A pool of bidirectional gRPC streams. Currently only used for GetPage streams.
/// TODO: consider making this generic over request and response types, but not currently needed.
///
/// Individual streams are not exposed to callers -- instead, callers can send invididual requests
/// to the pool and await a response. Internally, requests are multiplexed over streams and
/// channels.
pub struct StreamPool {
    /// gRPC client pool.
    clients: Arc<ClientPool>,
    /// All pooled streams.
    ///
    /// TODO: this must use something more sophisticated. This is on the GetPage hot path, so we
    /// want cheap concurrent access in the common case. We also want to prioritize using streams
    /// that belong to lower-numbered channels and clients first, such that we can reap
    /// higher-numbered channels and clients as they become idle. And we can't hold a lock on this
    /// while we're spinning up new streams, but we want to install an entry prior to spinning it up
    /// such that other requests can join onto it (we won't know the client/channel ID until we've
    /// acquired a client from the client pool which may block).
    streams: Arc<Mutex<HashMap<StreamID, StreamEntry>>>,
    /// Limits the max number of concurrent requests (not streams).
    limiter: Semaphore,
    /// Stream ID generator.
    next_stream_id: AtomicUsize,
}

type StreamID = usize;
type StreamSender = tokio::sync::mpsc::Sender<(GetPageRequest, ResponseSender)>;
type StreamReceiver = tokio::sync::mpsc::Receiver<(GetPageRequest, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tonic::Result<GetPageResponse>>;

struct StreamEntry {
    /// The request stream sender. The stream task exits when this is dropped.
    sender: StreamSender,
    /// Number of in-flight requests on this stream.
    queue_depth: Arc<AtomicUsize>,
}

impl StreamPool {
    /// Max number of concurrent requests per stream.
    const STREAM_QUEUE_DEPTH: usize = 2;
    /// Max number of concurrent requests in flight.
    const TOTAL_QUEUE_DEPTH: usize = ClientPool::CLIENT_LIMIT * Self::STREAM_QUEUE_DEPTH;

    /// Creates a new stream pool, using the given client pool.
    pub fn new(clients: Arc<ClientPool>) -> Self {
        Self {
            clients,
            streams: Arc::default(),
            limiter: Semaphore::new(Self::TOTAL_QUEUE_DEPTH),
            next_stream_id: AtomicUsize::default(),
        }
    }

    /// Sends a request via the stream pool, returning a response.
    pub async fn send(&self, req: GetPageRequest) -> tonic::Result<GetPageResponse> {
        // Acquire a permit. For simplicity, we drop it when this method returns, even if the
        // request is still in flight because the caller went away. We do the same for queue depth.
        let _permit = self.limiter.acquire().await.expect("never closed");

        // Acquire a stream from the pool.
        #[allow(clippy::await_holding_lock)] // TODO: Clippy doesn't understand drop()
        let (req_tx, queue_depth) = async {
            let mut streams = self.streams.lock().unwrap();

            // Try to find an existing stream with available capacity.
            for entry in streams.values() {
                if entry
                    .queue_depth
                    // TODO: review ordering.
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |depth| {
                        (depth < Self::STREAM_QUEUE_DEPTH).then_some(depth + 1)
                    })
                    .is_ok()
                {
                    return anyhow::Ok((entry.sender.clone(), entry.queue_depth.clone()));
                }
            }

            // No available stream, spin up a new one. We install the stream entry first and release
            // the lock. This will allow other requests to join onto this stream while we're
            // spinning up the task, and also create additional streams concurrently when full.
            let id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
            let queue_depth = Arc::new(AtomicUsize::new(1));
            let (req_tx, req_rx) = tokio::sync::mpsc::channel(Self::STREAM_QUEUE_DEPTH);
            streams.insert(
                id,
                StreamEntry {
                    sender: req_tx.clone(),
                    queue_depth: queue_depth.clone(),
                },
            );
            drop(streams); // drop lock before spinning up task

            let clients = self.clients.clone();
            let streams = self.streams.clone();

            tokio::spawn(async move {
                if let Err(err) = Self::run_stream(clients, req_rx).await {
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

        // Decrement the queue depth on return. We incremented it above, so we also decrement it
        // here, even though that could prematurely decrement it before the response arrives.
        defer!(queue_depth.fetch_sub(1, Ordering::SeqCst););

        // Send the request and wait for the response.
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        req_tx
            .send((req, resp_tx))
            .await
            .map_err(|_| tonic::Status::unavailable("stream closed"))?;

        resp_rx
            .await
            .map_err(|_| tonic::Status::unavailable("stream closed"))?
    }

    /// Runs a stream task.
    async fn run_stream(
        client_pool: Arc<ClientPool>,
        mut caller_rx: StreamReceiver,
    ) -> anyhow::Result<()> {
        // Acquire a client from the pool and create a stream.
        let mut client_guard = client_pool.get().await?;
        let client = client_guard.deref_mut();

        let (req_tx, req_rx) = tokio::sync::mpsc::channel(Self::STREAM_QUEUE_DEPTH);
        let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
        let mut resp_stream = client.get_pages(req_stream).await?;

        // Track caller response channels by request ID. If the task returns early, the response
        // channels will be dropped and the callers will receive an error.
        let mut callers = HashMap::with_capacity(Self::STREAM_QUEUE_DEPTH);

        // Process requests and responses.
        loop {
            tokio::select! {
                // Receive requests from callers and send them to the stream.
                req = caller_rx.recv() => {
                    // Shut down if input channel is closed.
                    let Some((req, resp_tx)) = req else {
                        return Ok(()); // stream closed
                    };

                    // Store the response channel by request ID.
                    if callers.contains_key(&req.request_id) {
                        _ = resp_tx.send(Err(tonic::Status::invalid_argument(
                            format!("duplicate request ID: {}", req.request_id),
                        )));
                        continue;
                    }
                    callers.insert(req.request_id, resp_tx);

                    // Send the request on the stream. Bail out on send errors.
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

                    // Send the response to the caller.
                    let Some(resp_tx) = callers.remove(&resp.request_id) else {
                        warn!("received response for unknown request ID: {}", resp.request_id);
                        continue;
                    };
                    _ = resp_tx.send(Ok(resp)); // ignore error if caller went away
                }
            }
        }
    }
}
