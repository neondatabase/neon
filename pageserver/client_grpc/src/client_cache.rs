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

/// A pooled gRPC client with capacity tracking and error handling.
pub struct ConnectionPool {
    inner: Mutex<Inner>,

    // Config options that apply to each connection
    endpoint: String,
    max_consumers: usize,
    error_threshold: usize,
    connect_timeout: Duration,
    connect_backoff: Duration,

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
}

/// A client borrowed from the pool.
pub struct PooledClient {
    pub channel: Channel,
    pool: Arc<ConnectionPool>,
    id: uuid::Uuid,
}

impl ConnectionPool {
    /// Create a new pool and spawn the background task that handles requests.
    pub fn new(
        endpoint: &String,
        max_consumers: usize,
        error_threshold: usize,
        connect_timeout: Duration,
        connect_backoff: Duration,
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
            error_threshold,
            connect_timeout,
            connect_backoff,
            request_tx,
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

        pool
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
            //
            // TODO: This would be more accurate if it waited for a timer, and the timer
            // was reset when a connection failed. Using timestamps, we may miss new failures
            // that occur while we are sleeping.
            //
            // TODO: Should the backoff be exponential?
            //
            if let Some(delay) = {
                let inner = self.inner.lock().await;
                inner.last_connect_failure.and_then(|at| {
                    (at.elapsed() < self.connect_backoff)
                        .then(|| self.connect_backoff - at.elapsed())
                })
            } {
                sleep(delay).await;
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
                    .connect(),
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
