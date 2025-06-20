use async_trait::async_trait;
use priority_queue::PriorityQueue;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use uuid;

#[async_trait]
pub trait PooledClientFactory<T>: Send + Sync + 'static {
    /// Create a new pooled item.
    async fn create(
        &self,
        connect_timeout: Duration,
    ) -> Result<Result<T, tonic::Status>, tokio::time::error::Elapsed>;
}

/// A pooled gRPC client with capacity tracking and error handling.
pub struct ClientCache<T> {
    inner: Mutex<Inner<T>>,

    fact: Arc<dyn PooledClientFactory<T> + Send + Sync>,

    connect_timeout: Duration,
    connect_backoff: Duration,

    /// The maximum number of consumers that can use a single connection.
    max_consumers: usize,

    /// The number of consecutive errors before a connection is removed from the pool.
    error_threshold: usize,

    /// The maximum duration a connection can be idle before being removed.
    max_idle_duration: Duration,
    max_total_connections: usize,

    client_semaphore: Arc<Semaphore>,
}

struct Inner<T> {
    entries: HashMap<uuid::Uuid, CacheEntry<T>>,
    pq: PriorityQueue<uuid::Uuid, usize>,
    // This is updated when a connection is dropped, or we fail
    // to create a new connection.
    last_connect_failure: Option<Instant>,
    waiters: usize,
    in_progress: usize,
}

struct CacheEntry<T> {
    client: T,
    active_consumers: usize,
    consecutive_errors: usize,
    last_used: Instant,
}

/// A client borrowed from the pool.
pub struct PooledClient<T> {
    pub client: T,
    pool: Arc<ClientCache<T>>,
    is_ok: bool,
    id: uuid::Uuid,
    permit: OwnedSemaphorePermit,
}

impl<T: Clone + Send + 'static> ClientCache<T> {
    pub fn new(
        fact: Arc<dyn PooledClientFactory<T> + Send + Sync>,
        connect_timeout: Duration,
        connect_backoff: Duration,
        max_consumers: usize,
        error_threshold: usize,
        max_idle_duration: Duration,
        max_total_connections: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::<T> {
                entries: HashMap::new(),
                pq: PriorityQueue::new(),
                last_connect_failure: None,
                waiters: 0,
                in_progress: 0,
            }),
            fact: Arc::clone(&fact),
            connect_timeout,
            connect_backoff,
            max_consumers,
            error_threshold,
            max_idle_duration,
            max_total_connections,
            client_semaphore: Arc::new(Semaphore::new(0)),
        })
    }
}

impl<T: Clone + Send + 'static> PooledClient<T> {
    pub fn client(&self) -> T {
        self.client.clone()
    }
}
