use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
    sync::Arc,
    time::{Duration, Instant},
};

use priority_queue::PriorityQueue;

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::sleep,
};
use tonic::transport::{Channel, Endpoint};

use uuid;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::future;
use rand::{Rng, SeedableRng, rngs::StdRng};

use bytes::BytesMut;
use http::Uri;
use hyper_util::rt::TokioIo;
use tower::service_fn;

use tokio_util::sync::CancellationToken;

//
// The "TokioTcp" is flakey TCP network for testing purposes, in order
// to simulate network errors and delays.
//

/// Wraps a `TcpStream`, buffers incoming data, and injects a random delay per fresh read/write.
pub struct TokioTcp {
    tcp: TcpStream,
    /// Maximum randomized delay in milliseconds
    delay_ms: u64,

    /// Next deadline instant for delay
    deadline: Instant,
    /// Internal buffer of previously-read data
    buffer: BytesMut,
}

impl TokioTcp {
    /// Create a new wrapper with given max delay (ms)
    pub fn new(stream: TcpStream, delay_ms: u64) -> Self {
        let initial = if delay_ms > 0 {
            rand::thread_rng().gen_range(0..delay_ms)
        } else {
            0
        };
        let deadline = Instant::now() + Duration::from_millis(initial);
        TokioTcp {
            tcp: stream,
            delay_ms,
            deadline,
            buffer: BytesMut::new(),
        }
    }
}

impl AsyncRead for TokioTcp {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Safe because TokioTcp is Unpin
        let this = self.get_mut();

        // 1) Drain any buffered data
        if !this.buffer.is_empty() {
            let to_copy = this.buffer.len().min(buf.remaining());
            buf.put_slice(&this.buffer.split_to(to_copy));
            return Poll::Ready(Ok(()));
        }

        // 2) If we're still before the deadline, schedule a wake and return Pending
        let now = Instant::now();
        if this.delay_ms > 0 && now < this.deadline {
            let waker = cx.waker().clone();
            let wait = this.deadline - now;
            tokio::spawn(async move {
                sleep(wait).await;
                waker.wake_by_ref();
            });
            return Poll::Pending;
        }

        // 3) Past deadline: compute next random deadline
        if this.delay_ms > 0 {
            let next_ms = rand::thread_rng().gen_range(0..=this.delay_ms);
            this.deadline = Instant::now() + Duration::from_millis(next_ms);
        }

        // 4) Perform actual read into a temporary buffer
        let mut tmp = [0u8; 4096];
        let mut rb = ReadBuf::new(&mut tmp);
        match Pin::new(&mut this.tcp).poll_read(cx, &mut rb) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let filled = rb.filled();
                if filled.is_empty() {
                    // EOF or zero bytes
                    Poll::Ready(Ok(()))
                } else {
                    this.buffer.extend_from_slice(filled);
                    let to_copy = this.buffer.len().min(buf.remaining());
                    buf.put_slice(&this.buffer.split_to(to_copy));
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for TokioTcp {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // 1) If before deadline, schedule wake and return Pending
        let now = Instant::now();
        if this.delay_ms > 0 && now < this.deadline {
            let waker = cx.waker().clone();
            let wait = this.deadline - now;
            tokio::spawn(async move {
                sleep(wait).await;
                waker.wake_by_ref();
            });
            return Poll::Pending;
        }

        // 2) Past deadline: compute next random deadline
        if this.delay_ms > 0 {
            let next_ms = rand::thread_rng().gen_range(0..=this.delay_ms);
            this.deadline = Instant::now() + Duration::from_millis(next_ms);
        }

        // 3) Actual write
        Pin::new(&mut this.tcp).poll_write(cx, data)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.tcp).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.tcp).poll_shutdown(cx)
    }
}

/// A pooled gRPC client with capacity tracking and error handling.
pub struct ConnectionPool {
    inner: Mutex<Inner>,

    // Config options that apply to each connection
    endpoint: String,
    max_consumers: usize,
    error_threshold: usize,
    connect_timeout: Duration,
    connect_backoff: Duration,

    // Parameters for testing
    max_delay_ms: u64,
    drop_rate: f64,
    hang_rate: f64,

    // The maximum duration a connection can be idle before being removed
    max_idle_duration: Duration,
    channel_semaphore: Arc<Semaphore>,

    shutdown_token: CancellationToken,
    aggregate_metrics: Option<Arc<crate::PageserverClientAggregateMetrics>>,
}

struct Inner {
    entries: HashMap<uuid::Uuid, ConnectionEntry>,
    pq: PriorityQueue<uuid::Uuid, usize>,
    // This is updated when a connection is dropped, or we fail
    // to create a new connection.
    last_connect_failure: Option<Instant>,
    waiters: usize,
    in_progress: usize,
}

struct ConnectionEntry {
    channel: Channel,
    active_consumers: usize,
    consecutive_errors: usize,
    last_used: Instant,
}

/// A client borrowed from the pool.
pub struct PooledClient {
    pub channel: Channel,
    pool: Arc<ConnectionPool>,
    id: uuid::Uuid,
    permit: OwnedSemaphorePermit,
}

impl ConnectionPool {
    pub fn new(
        endpoint: &String,
        max_consumers: usize,
        error_threshold: usize,
        connect_timeout: Duration,
        connect_backoff: Duration,
        max_idle_duration: Duration,
        max_delay_ms: u64,
        drop_rate: f64,
        hang_rate: f64,
        aggregate_metrics: Option<Arc<crate::PageserverClientAggregateMetrics>>,
    ) -> Arc<Self> {
        let shutdown_token = CancellationToken::new();
        let pool = Arc::new(Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                pq: PriorityQueue::new(),
                last_connect_failure: None,
                waiters: 0,
                in_progress: 0,
            }),
            channel_semaphore: Arc::new(Semaphore::new(0)),
            endpoint: endpoint.clone(),
            max_consumers: max_consumers,
            error_threshold: error_threshold,
            connect_timeout: connect_timeout,
            connect_backoff: connect_backoff,
            max_idle_duration: max_idle_duration,
            max_delay_ms: max_delay_ms,
            drop_rate: drop_rate,
            hang_rate: hang_rate,
            shutdown_token: shutdown_token.clone(),
            aggregate_metrics: aggregate_metrics.clone(),
        });

        // Cancelable background task to sweep idle connections
        let sweeper_token = shutdown_token.clone();
        let sweeper_pool = Arc::clone(&pool);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sweeper_token.cancelled() => break,
                    _ = async {
                        sweeper_pool.sweep_idle_connections().await;
                        sleep(Duration::from_secs(5)).await;
                    } => {}
                }
            }
        });

        pool
    }

    pub async fn shutdown(self: Arc<Self>) {
        self.shutdown_token.cancel();

        loop {
            let all_idle = {
                let inner = self.inner.lock().await;
                inner.entries.values().all(|e| e.active_consumers == 0)
            };
            if all_idle {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // 4. Remove all entries
        let mut inner = self.inner.lock().await;
        inner.entries.clear();
    }

    /// Sweep and remove idle connections safely, burning their permits.
    async fn sweep_idle_connections(self: &Arc<Self>) {
        let mut ids_to_remove = Vec::new();
        let now = Instant::now();

        // Remove idle entries. First collect permits for those connections so that
        // no consumer will reserve them, then remove them from the pool.
        {
            let mut inner = self.inner.lock().await;
            inner.entries.retain(|id, entry| {
                if entry.active_consumers == 0
                    && now.duration_since(entry.last_used) > self.max_idle_duration
                {
                    // metric
                    match self.aggregate_metrics {
                        Some(ref metrics) => {
                            metrics
                                .retry_counters
                                .with_label_values(&["connection_swept"])
                                .inc();
                        }
                        None => {}
                    }
                    ids_to_remove.push(*id);
                    return false; // remove this entry
                }
                true
            });
            // Remove the entries from the priority queue
            for id in ids_to_remove {
                inner.pq.remove(&id);
            }
        }
    }

    // If we have a permit already, get a connection out of the heap
    async fn get_conn_with_permit(
        self: Arc<Self>,
        permit: OwnedSemaphorePermit,
    ) -> Option<PooledClient> {
        let mut inner = self.inner.lock().await;

        // Pop the highest-active-consumers connection. There are no connections
        // in the heap that have more than max_consumers active consumers.
        if let Some((id, _cons)) = inner.pq.pop() {
            let entry = inner
                .entries
                .get_mut(&id)
                .expect("pq and entries got out of sync");

            let mut active_consumers = entry.active_consumers;
            entry.active_consumers += 1;
            entry.last_used = Instant::now();

            let client = PooledClient {
                channel: entry.channel.clone(),
                pool: Arc::clone(&self),
                id,
                permit: permit,
            };

            // re‐insert with updated priority
            active_consumers += 1;
            if active_consumers < self.max_consumers {
                inner.pq.push(id, active_consumers as usize);
            }
            return Some(client);
        } else {
            // If there is no connection to take, it is because permits for a connection
            // need to drain. This can happen if a connection is removed because it has
            // too many errors. It is taken out of the heap/hash table in this case, but
            // we can't remove it's permits until now.
            //
            // Just forget the permit and retry.
            permit.forget();
            return None;
        }
    }

    pub async fn get_client(self: Arc<Self>) -> Result<PooledClient, tonic::Status> {
        // The pool is shutting down. Don't accept new connections.
        if self.shutdown_token.is_cancelled() {
            return Err(tonic::Status::unavailable("Pool is shutting down"));
        }

        // A loop is necessary because when a connection is draining, we have to return
        // a permit and retry.
        loop {
            let self_clone = Arc::clone(&self);
            let mut semaphore = Arc::clone(&self_clone.channel_semaphore);

            match semaphore.try_acquire_owned() {
                Ok(permit_) => {
                    // We got a permit, so check the heap for a connection
                    // we can use.
                    let pool_conn = self_clone.get_conn_with_permit(permit_).await;
                    match pool_conn {
                        Some(pool_conn_) => {
                            return Ok(pool_conn_);
                        }
                        None => {
                            // No connection available. Forget the permit and retry.
                            continue;
                        }
                    }
                }
                Err(_) => {
                    match self_clone.aggregate_metrics {
                        Some(ref metrics) => {
                            metrics
                                .retry_counters
                                .with_label_values(&["sema_acquire_failed"])
                                .inc();
                        }
                        None => {}
                    }

                    {
                        //
                        // This is going to generate enough connections to handle a burst,
                        // but it may generate up to twice the number of connections needed
                        // in the worst case. Extra connections will go idle and be cleaned
                        // up.
                        //
                        let mut inner = self_clone.inner.lock().await;
                        inner.waiters += 1;
                        if inner.waiters >= (inner.in_progress * self_clone.max_consumers) {
                            let self_clone_spawn = Arc::clone(&self_clone);
                            tokio::task::spawn(async move {
                                self_clone_spawn.create_connection().await;
                            });
                            inner.in_progress += 1;
                        }
                    }
                    // Wait for a connection to become available, either because it
                    // was created or because a connection was returned to the pool
                    // by another consumer.
                    semaphore = Arc::clone(&self_clone.channel_semaphore);
                    let conn_permit = semaphore.acquire_owned().await.unwrap();
                    {
                        let mut inner = self_clone.inner.lock().await;
                        inner.waiters -= 1;
                    }
                    // We got a permit, check the heap for a connection.
                    let pool_conn = self_clone.get_conn_with_permit(conn_permit).await;
                    match pool_conn {
                        Some(pool_conn_) => {
                            return Ok(pool_conn_);
                        }
                        None => {
                            // No connection was found, forget the permit and retry.
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn create_connection(&self) -> () {
        let max_delay_ms = self.max_delay_ms;
        let drop_rate = self.drop_rate;
        let hang_rate = self.hang_rate;

        // This is a custom connector that inserts delays and errors, for
        // testing purposes. It would normally be disabled by the config.
        let connector = service_fn(move |uri: Uri| {
            let drop_rate = drop_rate;
            let hang_rate = hang_rate;
            async move {
                let mut rng = StdRng::from_entropy();
                // Simulate an indefinite hang
                if hang_rate > 0.0 && rng.gen_bool(hang_rate) {
                    // never completes, to test timeout
                    return future::pending::<Result<TokioIo<TokioTcp>, std::io::Error>>().await;
                }

                // Random drop (connect error)
                if drop_rate > 0.0 && rng.gen_bool(drop_rate) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "simulated connect drop",
                    ));
                }

                // Otherwise perform real TCP connect
                let addr = match (uri.host(), uri.port()) {
                    // host + explicit port
                    (Some(host), Some(port)) => format!("{}:{}", host, port.as_str()),
                    // host only (no port)
                    (Some(host), None) => host.to_string(),
                    // neither? error out
                    _ => return Err(Error::new(ErrorKind::InvalidInput, "no host or port")),
                };

                let tcp = TcpStream::connect(addr).await?;
                let tcpwrapper = TokioTcp::new(tcp, max_delay_ms);
                Ok(TokioIo::new(tcpwrapper))
            }
        });

        // Generate a random backoff to add some jitter so that connections
        // don't all retry at the same time.
        let mut backoff_delay = Duration::from_millis(
            rand::thread_rng().gen_range(0..=self.connect_backoff.as_millis() as u64),
        );

        loop {
            if self.shutdown_token.is_cancelled() {
                return;
            }

            // Back off.
            // Loop because failure can occur while we are sleeping, so wait
            // until the failure stopped for at least one backoff period. Backoff
            // period includes some jitter, so that if multiple connections are
            // failing, they don't all retry at the same time.
            loop {
                if let Some(delay) = {
                    let inner = self.inner.lock().await;
                    inner.last_connect_failure.and_then(|at| {
                        (at.elapsed() < backoff_delay).then(|| backoff_delay - at.elapsed())
                    })
                } {
                    sleep(delay).await;
                } else {
                    break; // No delay, so we can create a connection
                }
            }

            //
            // Create a new connection.
            //
            // The connect timeout is also the timeout for an individual gRPC request
            // on this connection. (Requests made later on this channel will time out
            // with the same timeout.)
            //
            match self.aggregate_metrics {
                Some(ref metrics) => {
                    metrics
                        .retry_counters
                        .with_label_values(&["connection_attempt"])
                        .inc();
                }
                None => {}
            }

            let attempt = tokio::time::timeout(
                self.connect_timeout,
                Endpoint::from_shared(self.endpoint.clone())
                    .expect("invalid endpoint")
                    .timeout(self.connect_timeout)
                    .connect_with_connector(connector),
            )
            .await;

            match attempt {
                // Connection succeeded
                Ok(Ok(channel)) => {
                    {
                        match self.aggregate_metrics {
                            Some(ref metrics) => {
                                metrics
                                    .retry_counters
                                    .with_label_values(&["connection_success"])
                                    .inc();
                            }
                            None => {}
                        }
                        let mut inner = self.inner.lock().await;
                        let id = uuid::Uuid::new_v4();
                        inner.entries.insert(
                            id,
                            ConnectionEntry {
                                channel: channel.clone(),
                                active_consumers: 0,
                                consecutive_errors: 0,
                                last_used: Instant::now(),
                            },
                        );
                        inner.pq.push(id, 0);
                        inner.in_progress -= 1;
                        self.channel_semaphore.add_permits(self.max_consumers);
                        return;
                    };
                }
                // Connection failed, back off and retry
                Ok(Err(_)) | Err(_) => {
                    match self.aggregate_metrics {
                        Some(ref metrics) => {
                            metrics
                                .retry_counters
                                .with_label_values(&["connect_failed"])
                                .inc();
                        }
                        None => {}
                    }
                    let mut inner = self.inner.lock().await;
                    inner.last_connect_failure = Some(Instant::now());
                    // Add some jitter so that every connection doesn't retry at once
                    let jitter = rand::thread_rng().gen_range(0..=backoff_delay.as_millis() as u64);
                    backoff_delay =
                        Duration::from_millis(backoff_delay.as_millis() as u64 + jitter);

                    // Do not backoff longer than one minute
                    if backoff_delay > Duration::from_secs(60) {
                        backoff_delay = Duration::from_secs(60);
                    }
                    // continue the loop to retry
                }
            }
        }
    }

    /// Return client to the pool, indicating success or error.
    pub async fn return_client(&self, id: uuid::Uuid, success: bool, permit: OwnedSemaphorePermit) {
        let mut inner = self.inner.lock().await;
        if let Some(entry) = inner.entries.get_mut(&id) {
            entry.last_used = Instant::now();
            if entry.active_consumers <= 0 {
                panic!("A consumer completed when active_consumers was zero!")
            }
            entry.active_consumers = entry.active_consumers - 1;
            if success {
                if entry.consecutive_errors < self.error_threshold {
                    entry.consecutive_errors = 0;
                }
            } else {
                entry.consecutive_errors += 1;
                if entry.consecutive_errors == self.error_threshold {
                    match self.aggregate_metrics {
                        Some(ref metrics) => {
                            metrics
                                .retry_counters
                                .with_label_values(&["connection_dropped"])
                                .inc();
                        }
                        None => {}
                    }
                }
            }

            //
            // Too many errors on this connection. If there are no active users,
            // remove it. Otherwise just wait for active_consumers to go to zero.
            // This connection will not be selected for new consumers.
            //
            let active_consumers = entry.active_consumers;
            if entry.consecutive_errors >= self.error_threshold {
                // too many errors, remove the connection permanently. Once it drains,
                // it will be dropped.
                if inner.pq.get_priority(&id).is_some() {
                    inner.pq.remove(&id);
                }

                inner.last_connect_failure = Some(Instant::now());

                // The connection has been removed, it's permits will be
                // drained because if we look for a connection and it's not there
                // we just forget the permit. However, this process can be a little
                // bit faster if we just forget permits as the connections are returned.
                permit.forget();
            } else {
                // update its priority in the queue
                if inner.pq.get_priority(&id).is_some() {
                    inner.pq.change_priority(&id, active_consumers);
                } else {
                    // This connection is not in the heap, but it has space
                    // for more consumers. Put it back in the heap.
                    if active_consumers < self.max_consumers {
                        inner.pq.push(id, active_consumers);
                    }
                }
            }
        }
        // The semaphore permit is released when the pooled client is dropped.
    }
}

impl PooledClient {
    pub fn channel(&self) -> Channel {
        return self.channel.clone();
    }

    pub async fn finish(self, result: Result<(), tonic::Status>) {
        self.pool
            .return_client(self.id, result.is_ok(), self.permit)
            .await;
    }
}
