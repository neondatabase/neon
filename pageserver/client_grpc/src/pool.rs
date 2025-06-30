use std::collections::{BTreeMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use std::future::Future;
use std::pin::Pin;
use tokio::sync::{Semaphore, SemaphorePermit};
use tonic::transport::{Channel, Endpoint};

/// Constructs new pool items.
/// TODO: use a proper error type.
type Maker<T> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = anyhow::Result<T>>>> + Send + Sync>;

/// A resource pool. This is used to manage gRPC channels, clients, and stream.
///
/// An item is only handed out to a single user at a time. New items will be created up to the pool
/// limit, if specified.
pub struct Pool<T> {
    /// Creates new pool items.
    maker: Maker<T>,
    /// Idle items in the pool. Returned items are pushed to the front of the queue, so that the
    /// oldest idle items are kept at the back.
    ///
    /// TODO: reap idle items after some time.
    /// TODO: consider prewarming items.
    idle: Arc<Mutex<VecDeque<T>>>,
    /// Limits the max number of items managed by the pool.
    limiter: Semaphore,
}

impl<T> Pool<T> {
    /// Create a new pool with the specified limit.
    pub fn new(maker: Maker<T>, limit: Option<usize>) -> Self {
        Self {
            maker,
            idle: Default::default(),
            limiter: Semaphore::new(limit.unwrap_or(Semaphore::MAX_PERMITS)),
        }
    }

    /// Gets an item from the pool, or creates a new one if necessary. Blocks if the pool is at its
    /// limit. The item is returned to the pool when the guard is dropped.
    pub async fn get(&self) -> anyhow::Result<PoolGuard<T>> {
        let permit = self.limiter.acquire().await.expect("never closed");

        // Acquire an idle item from the pool, or create a new one.
        let item = self.idle.lock().unwrap().pop_front();
        let item = match item {
            Some(item) => item,
            // TODO: if an item is returned while we're waiting, use the returned item instead.
            None => (self.maker)().await?,
        };

        Ok(PoolGuard {
            pool: self,
            permit,
            item: Some(item),
        })
    }
}

/// A guard for a pooled item.
pub struct PoolGuard<'a, T> {
    pool: &'a Pool<T>,
    permit: SemaphorePermit<'a>,
    item: Option<T>, // only None during drop
}

impl<T> Deref for PoolGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().expect("not dropped")
    }
}

impl<T> DerefMut for PoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item.as_mut().expect("not dropped")
    }
}

impl<T> Drop for PoolGuard<'_, T> {
    fn drop(&mut self) {
        // Return the item to the pool.
        self.pool
            .idle
            .lock()
            .unwrap()
            .push_front(self.item.take().expect("only dropped once"));
        // The permit will be returned by its drop handler. Tag it here for visibility.
        _ = self.permit;
    }
}

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
    channels: Arc<Mutex<BTreeMap<ChannelID, ChannelEntry>>>,
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
    const CLIENTS_PER_CHANNEL: usize = 20;

    /// Creates a new channel pool for the given Pageserver URL.
    pub fn new(url: String) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: Endpoint::from_shared(url)?,
            channels: Default::default(),
        })
    }

    /// Acquires a new gRPC channel.
    ///
    /// NB: this is not particularly performance-sensitive. It is called rarely since clients are
    /// cached and reused by ClientPool, and the number of channels will be small. O(n) performance
    /// is therefore okay.
    pub fn get(&self) -> anyhow::Result<ChannelGuard<'_>> {
        let mut channels = self.channels.lock().unwrap();

        // Find an existing channel with available capacity. We check entries in BTreeMap order,
        // such that we fill up the earliest channels first. The ClientPool also uses lower-ordered
        // channels first. This allows us to reap later channels as they become idle.
        for (&id, entry) in channels.iter_mut() {
            if entry.clients < Self::CLIENTS_PER_CHANNEL {
                entry.clients += 1;
                return Ok(ChannelGuard {
                    pool: self,
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
            pool: self,
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

struct ChannelGuard<'a> {
    pool: &'a ChannelPool,
    id: ChannelID,
    channel: Option<Channel>,
}

impl<'a> ChannelGuard<'a> {
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
impl Drop for ChannelGuard<'_> {
    fn drop(&mut self) {
        let mut channels = self.pool.channels.lock().unwrap();
        let entry = channels.get_mut(&self.id).expect("unknown channel");
        assert!(entry.clients > 0, "channel clients underflow");
        entry.clients -= 1;
    }
}
