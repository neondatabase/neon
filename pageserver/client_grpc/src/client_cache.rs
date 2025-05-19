use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Notify, mpsc, watch},
    time::sleep,
};
use tonic::transport::{Channel, Endpoint};

use uuid;
use std::io::{self, Error, ErrorKind};
use std::{pin::Pin, task::{Context, Poll}};
use futures::future;
use rand::{Rng, rngs::StdRng, SeedableRng};
use tower::service_fn;
use http::Uri;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use bytes::BytesMut;

/// A pooled gRPC client with capacity tracking and error handling.
pub struct ConnectionPool {
    inner: Mutex<Inner>,

    // Config options that apply to each connection
    endpoint: String,
    max_consumers: usize,
    error_threshold: usize,
    connect_timeout: Duration,
    connect_backoff: Duration,
    // add max_delay
    // The maximum time a connection can be idle before being removed
    max_delay_ms: u64,
    drop_rate: f64,
    hang_rate: f64,

    // The maximum duration a connection can be idle before being removed
    max_idle_duration: Duration,

    // This notify is signaled when a connection is released or created.
    notify: Notify,

    // When it is time to create a new connection for the pool, we signal
    // a watch and a connection creation async wakes up and does the work.
    cc_watch_tx: watch::Sender<bool>,
    cc_watch_rx: watch::Receiver<bool>,

    // To acquire a connection from the pool, send a request
    // to this mpsc, and wait for a response.
    request_tx: mpsc::Sender<mpsc::Sender<PooledClient>>,
}

struct Inner {
    entries: HashMap<uuid::Uuid, ConnectionEntry>,

    // This is updated when a connection is dropped, or we fail
    // to create a new connection.
    last_connect_failure: Option<Instant>,
}

struct ConnectionEntry {
    channel: Channel,
    active_consumers: usize,
    consecutive_successes: usize,
    consecutive_errors: usize,
    last_used: Instant,
}

/// A client borrowed from the pool.
pub struct PooledClient {
    pub channel: Channel,
    pool: Arc<ConnectionPool>,
    id: uuid::Uuid,
}
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

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.tcp).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.tcp).poll_shutdown(cx)
    }
}

impl ConnectionPool {
    /// Create a new pool and spawn the background task that handles requests.
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
    ) -> Arc<Self> {
        let (request_tx, mut request_rx) = mpsc::channel::<mpsc::Sender<PooledClient>>(100);
        let (watch_tx, watch_rx) = watch::channel(false);
        let pool = Arc::new(Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                last_connect_failure: None,
            }),
            notify: Notify::new(),
            cc_watch_tx: watch_tx,
            cc_watch_rx: watch_rx,
            endpoint: endpoint.clone(),
            max_consumers: max_consumers,
            error_threshold: error_threshold,
            connect_timeout: connect_timeout,
            connect_backoff: connect_backoff,
            max_idle_duration: max_idle_duration,
            request_tx: request_tx,
            max_delay_ms: max_delay_ms,
            drop_rate: drop_rate,
            hang_rate: hang_rate,
        });

        //
        // Background task to handle requests and create connections.
        //
        // TODO: These should be canceled when the ConnectionPool is dropped
        //

        let bg_cc_pool = Arc::clone(&pool);
        tokio::spawn(async move {
            loop {
                bg_cc_pool.create_connection().await;
            }
        });

        let bg_pool = Arc::clone(&pool);
        tokio::spawn(async move {
            while let Some(responder) = request_rx.recv().await {
                // TODO: This call should time out and return an error
                let (id, channel) = bg_pool.acquire_connection().await;
                let client = PooledClient {
                    channel,
                    pool: Arc::clone(&bg_pool),
                    id,
                };
                let _ = responder.send(client).await;
            }
        });

        // Background task to sweep idle connections
        let sweeper_pool = Arc::clone(&pool);
        tokio::spawn(async move {
            loop {
                sweeper_pool.sweep_idle_connections().await;
                sleep(Duration::from_secs(5)).await; // Run every 5 seconds
            }
        });

        pool
    }

    // Sweep and remove idle connections
    async fn sweep_idle_connections(&self) {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        inner.entries.retain(|_id, entry| {
            if entry.active_consumers == 0 && now.duration_since(entry.last_used) > self.max_idle_duration {
                // Remove idle connection
                return false;
            }
            true
        });
    }


    async fn acquire_connection(&self) -> (uuid::Uuid, Channel) {
        loop {
            // Reuse an existing healthy connection if available
            {
                let mut inner = self.inner.lock().await;
                // TODO: Use a heap, although the number of connections is small
                if let Some((&id, entry)) = inner
                    .entries
                    .iter_mut()
                    .filter(|(_, e)| e.active_consumers < self.max_consumers)
                    .filter(|(_, e)| e.consecutive_errors < self.error_threshold)
                    .max_by_key(|(_, e)| e.active_consumers)
                {
                    entry.active_consumers += 1;
                    return (id, entry.channel.clone());
                }
                // There is no usable connection, so notify the connection creation async to make one. (It is
                // possible that a consumer will release a connection while the new one is being created, in
                // which case we will use it right away, but the new connection will be created anyway.)
                let _ = self.cc_watch_tx.send(true);
            }
            // Wait for a new connection, or for one of the consumers to release a connection
            // TODO: Put this notify in a timeout
            self.notify.notified().await;
        }
    }

    async fn create_connection(&self) -> () {

        let max_delay_ms = self.max_delay_ms;
        let drop_rate = self.drop_rate;
        let hang_rate = self.hang_rate;

        // This is a custom connector that inserts delays and errors, for
        // testing purposes. It would normally be disabled by the config.
        let connector = service_fn(move |uri: Uri| {
            let max_delay = max_delay_ms;
            let drop_rate = drop_rate;
            let hang_rate = hang_rate;
            async move {
                let mut rng = StdRng::from_entropy();
                // Simulate an indefinite hang
                if hang_rate > 0.0 && rng.gen_bool(hang_rate) {
                    // never completes, to test timeout
                    return future::pending::<Result<TokioIo<TokioTcp>, std::io::Error>>().await;
                }

                if max_delay > 0 {
                    // Random delay before connecting
                    let delay = rng.gen_range(0..max_delay);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
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
                    (Some(host), None)      => host.to_string(),
                    // neither? error out
                    _ => return Err(Error::new(ErrorKind::InvalidInput, "no host or port")),
                };

                //let addr = uri.authority().unwrap().as_str();
                let tcp = TcpStream::connect(addr).await?;
                let tcpwrapper = TokioTcp::new(
                    tcp,
                    max_delay_ms,
                );
                Ok(TokioIo::new(tcpwrapper))
            }
        });

        // Wait to be signalled to create a connection.
        let mut recv = self.cc_watch_tx.subscribe();
        if !*self.cc_watch_rx.borrow() {
            while recv.changed().await.is_ok() {
                if *self.cc_watch_rx.borrow() {
                    break;
                }
            }
        }

        loop {
            loop {
                if let Some(delay) = {
                    let inner = self.inner.lock().await;
                    inner.last_connect_failure.and_then(|at| {
                        (at.elapsed() < self.connect_backoff)
                            .then(|| self.connect_backoff - at.elapsed())
                    })
                } {
                    sleep(delay).await;
                } else {
                    break   // No delay, so we can create a connection
                }
            }

            //
            // Create a new connection.
            //
            // The connect timeout is also the timeout for an individual gRPC request
            // on this connection. (Requests made later on this channel will time out
            // with the same timeout.)
            //
            let attempt = tokio::time::timeout(
                self.connect_timeout,
                Endpoint::from_shared(self.endpoint.clone())
                    .expect("invalid endpoint")
                    .timeout(self.connect_timeout)
                    .connect_with_connector(connector)
            )
            .await;


            match attempt {
                Ok(Ok(channel)) => {
                    {

                        let mut inner = self.inner.lock().await;
                        let id = uuid::Uuid::new_v4();
                        inner.entries.insert(
                            id,
                            ConnectionEntry {
                                channel: channel.clone(),
                                active_consumers: 0,
                                consecutive_successes: 0,
                                consecutive_errors: 0,
                                last_used: Instant::now(),
                            },
                        );
                        self.notify.notify_one();
                        let _ = self.cc_watch_tx.send(false);
                        return;
                    };
                }
                Ok(Err(_)) | Err(_) => {
                    let mut inner = self.inner.lock().await;
                    inner.last_connect_failure = Some(Instant::now());
                }
            }
        }
    }

    /// Get a client we can use to send gRPC messages.
    pub async fn get_client(&self) -> PooledClient {
        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        self.request_tx
            .send(resp_tx)
            .await
            .expect("ConnectionPool task has shut down");
        resp_rx
            .recv()
            .await
            .expect("ConnectionPool task has shut down")
    }

    /// Return client to the pool, indicating success or error.
    pub async fn return_client(&self, id: uuid::Uuid, success: bool) {
        let mut inner = self.inner.lock().await;
        let mut new_failure = false;
        if let Some(entry) = inner.entries.get_mut(&id) {
            entry.last_used = Instant::now();
            // TODO: This should be a debug_assert
            if entry.active_consumers <= 0 {
                panic!("A consumer completed when active_consumers was zero!")
            }
            entry.active_consumers = entry.active_consumers - 1;
            if entry.consecutive_errors < self.error_threshold {
                if success {
                    entry.consecutive_successes += 1;
                    entry.consecutive_errors = 0;
                } else {
                    entry.consecutive_errors += 1;
                    entry.consecutive_successes = 0;
                    if entry.consecutive_errors == self.error_threshold {
                        new_failure = true;
                    }
                }
            }
            //
            // Too many errors on this connection. If there are no active users,
            // remove it. Otherwise just wait for active_consumers to go to zero.
            // This connection will not be selected for new consumers.
            //
            if entry.consecutive_errors == self.error_threshold {
                let remove = entry.active_consumers;
                if new_failure {
                    inner.last_connect_failure = Some(Instant::now());
                }
                if remove == 0 {
                    inner.entries.remove(&id);
                }
            } else {
                self.notify.notify_one();
            }
        }
    }
}

impl PooledClient {
    pub fn channel(&self) -> Channel {
        return self.channel.clone();
    }

    pub async fn finish(self, result: Result<(), tonic::Status>) {
        self.pool.return_client(self.id, result.is_ok()).await;
    }
}
