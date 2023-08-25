use anyhow::Context;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::poll_fn;
use parking_lot::RwLock;
use pbkdf2::{
    password_hash::{PasswordHashString, PasswordHasher, PasswordVerifier, SaltString},
    Params, Pbkdf2,
};
use pq_proto::StartupMessageParams;
use std::sync::atomic::{self, AtomicUsize};
use std::{collections::HashMap, sync::Arc};
use std::{
    fmt,
    task::{ready, Poll},
};
use tokio::time;
use tokio_postgres::AsyncMessage;

use crate::{auth, console};
use crate::{compute, config};

use super::sql_over_http::MAX_RESPONSE_SIZE;

use crate::proxy::ConnectMechanism;

use tracing::{error, warn};
use tracing::{info, info_span, Instrument};

pub const APP_NAME: &str = "sql_over_http";
const MAX_CONNS_PER_ENDPOINT: usize = 20;

#[derive(Debug)]
pub struct ConnInfo {
    pub username: String,
    pub dbname: String,
    pub hostname: String,
    pub password: String,
}

impl ConnInfo {
    // hm, change to hasher to avoid cloning?
    pub fn db_and_user(&self) -> (String, String) {
        (self.dbname.clone(), self.username.clone())
    }
}

impl fmt::Display for ConnInfo {
    // use custom display to avoid logging password
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}/{}", self.username, self.hostname, self.dbname)
    }
}

struct ConnPoolEntry {
    conn: Client,
    _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub struct EndpointConnPool {
    pools: HashMap<(String, String), DbUserConnPool>,
    total_conns: usize,
}

/// 4096 is the number of rounds that SCRAM-SHA-256 recommends.
/// It's not the 600,000 that OWASP recommends... but our passwords are high entropy anyway.
///
/// Still takes 1.4ms to hash on my hardware.
/// We don't want to ruin the latency improvements of using the pool by making password verification take too long
const PARAMS: Params = Params {
    rounds: 4096,
    output_length: 32,
};

#[derive(Default)]
pub struct DbUserConnPool {
    conns: Vec<ConnPoolEntry>,
    password_hash: Option<PasswordHashString>,
}

pub struct GlobalConnPool {
    // endpoint -> per-endpoint connection pool
    //
    // That should be a fairly conteded map, so return reference to the per-endpoint
    // pool as early as possible and release the lock.
    global_pool: DashMap<String, Arc<RwLock<EndpointConnPool>>>,

    /// [`DashMap::len`] iterates over all inner pools and acquires a read lock on each.
    /// That seems like far too much effort, so we're using a relaxed increment counter instead.
    /// It's only used for diagnostics.
    global_pool_size: AtomicUsize,

    // Maximum number of connections per one endpoint.
    // Can mix different (dbname, username) connections.
    // When running out of free slots for a particular endpoint,
    // falls back to opening a new connection for each request.
    max_conns_per_endpoint: usize,

    proxy_config: &'static crate::config::ProxyConfig,

    // Using a lock to remove any race conditions.
    // Eg cleaning up connections while a new connection is returned
    closed: RwLock<bool>,
}

impl GlobalConnPool {
    pub fn new(config: &'static crate::config::ProxyConfig) -> Arc<Self> {
        Arc::new(Self {
            global_pool: DashMap::new(),
            global_pool_size: AtomicUsize::new(0),
            max_conns_per_endpoint: MAX_CONNS_PER_ENDPOINT,
            proxy_config: config,
            closed: RwLock::new(false),
        })
    }

    pub fn shutdown(&self) {
        *self.closed.write() = true;

        self.global_pool.retain(|_, endpoint_pool| {
            let mut pool = endpoint_pool.write();
            // by clearing this hashmap, we remove the slots that a connection can be returned to.
            // when returning, it drops the connection if the slot doesn't exist
            pool.pools.clear();
            pool.total_conns = 0;

            false
        });
    }

    pub async fn get(
        &self,
        conn_info: &ConnInfo,
        force_new: bool,
        session_id: uuid::Uuid,
    ) -> anyhow::Result<Client> {
        let mut client: Option<Client> = None;

        let mut hash_valid = false;
        if !force_new {
            let pool = self.get_or_create_endpoint_pool(&conn_info.hostname);
            let mut hash = None;

            // find a pool entry by (dbname, username) if exists
            {
                let pool = pool.read();
                if let Some(pool_entries) = pool.pools.get(&conn_info.db_and_user()) {
                    if !pool_entries.conns.is_empty() {
                        hash = pool_entries.password_hash.clone();
                    }
                }
            }

            // a connection exists in the pool, verify the password hash
            if let Some(hash) = hash {
                let pw = conn_info.password.clone();
                let validate = tokio::task::spawn_blocking(move || {
                    Pbkdf2.verify_password(pw.as_bytes(), &hash.password_hash())
                })
                .await?;

                // if the hash is invalid, don't error
                // we will continue with the regular connection flow
                if validate.is_ok() {
                    hash_valid = true;
                    let mut pool = pool.write();
                    if let Some(pool_entries) = pool.pools.get_mut(&conn_info.db_and_user()) {
                        if let Some(entry) = pool_entries.conns.pop() {
                            client = Some(entry.conn);
                            pool.total_conns -= 1;
                        }
                    }
                }
            }
        }

        // ok return cached connection if found and establish a new one otherwise
        let new_client = if let Some(client) = client {
            if client.inner.is_closed() {
                info!("pool: cached connection '{conn_info}' is closed, opening a new one");
                connect_to_compute(self.proxy_config, conn_info, session_id).await
            } else {
                info!("pool: reusing connection '{conn_info}'");
                client.session.send(session_id)?;
                return Ok(client);
            }
        } else {
            info!("pool: opening a new connection '{conn_info}'");
            connect_to_compute(self.proxy_config, conn_info, session_id).await
        };

        match &new_client {
            // clear the hash. it's no longer valid
            // TODO: update tokio-postgres fork to allow access to this error kind directly
            Err(err)
                if hash_valid && err.to_string().contains("password authentication failed") =>
            {
                let pool = self.get_or_create_endpoint_pool(&conn_info.hostname);
                let mut pool = pool.write();
                if let Some(entry) = pool.pools.get_mut(&conn_info.db_and_user()) {
                    entry.password_hash = None;
                }
            }
            // new password is valid and we should insert/update it
            Ok(_) if !force_new && !hash_valid => {
                let pw = conn_info.password.clone();
                let new_hash = tokio::task::spawn_blocking(move || {
                    let salt = SaltString::generate(rand::rngs::OsRng);
                    Pbkdf2
                        .hash_password_customized(pw.as_bytes(), None, None, PARAMS, &salt)
                        .map(|s| s.serialize())
                })
                .await??;

                let pool = self.get_or_create_endpoint_pool(&conn_info.hostname);
                let mut pool = pool.write();
                pool.pools
                    .entry(conn_info.db_and_user())
                    .or_default()
                    .password_hash = Some(new_hash);
            }
            _ => {}
        }

        new_client
    }

    pub fn put(&self, conn_info: &ConnInfo, client: Client) -> anyhow::Result<()> {
        // We want to hold this open while we return. This ensures that the pool can't close
        // while we are in the middle of returning the connection.
        let closed = self.closed.read();
        if *closed {
            info!("pool: throwing away connection '{conn_info}' because pool is closed");
            return Ok(());
        }

        if client.inner.is_closed() {
            info!("pool: throwing away connection '{conn_info}' because connection is closed");
            return Ok(());
        }

        let pool = self.get_or_create_endpoint_pool(&conn_info.hostname);

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < self.max_conns_per_endpoint {
                // we create this db-user entry in get, so it should not be None
                if let Some(pool_entries) = pool.pools.get_mut(&conn_info.db_and_user()) {
                    pool_entries.conns.push(ConnPoolEntry {
                        conn: client,
                        _last_access: std::time::Instant::now(),
                    });

                    returned = true;
                    per_db_size = pool_entries.conns.len();

                    pool.total_conns += 1;
                }
            }

            pool.total_conns
        };

        // do logging outside of the mutex
        if returned {
            info!("pool: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!("pool: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }

        Ok(())
    }

    fn get_or_create_endpoint_pool(&self, endpoint: &String) -> Arc<RwLock<EndpointConnPool>> {
        // fast path
        if let Some(pool) = self.global_pool.get(endpoint) {
            return pool.clone();
        }

        // slow path
        let new_pool = Arc::new(RwLock::new(EndpointConnPool {
            pools: HashMap::new(),
            total_conns: 0,
        }));

        // find or create a pool for this endpoint
        let mut created = false;
        let pool = self
            .global_pool
            .entry(endpoint.clone())
            .or_insert_with(|| {
                created = true;
                new_pool
            })
            .clone();

        // log new global pool size
        if created {
            let global_pool_size = self
                .global_pool_size
                .fetch_add(1, atomic::Ordering::Relaxed)
                + 1;
            info!(
                "pool: created new pool for '{endpoint}', global pool size now {global_pool_size}"
            );
        }

        pool
    }
}

struct TokioMechanism<'a> {
    conn_info: &'a ConnInfo,
    session_id: uuid::Uuid,
}

#[async_trait]
impl ConnectMechanism for TokioMechanism<'_> {
    type Connection = Client;
    type ConnectError = tokio_postgres::Error;
    type Error = anyhow::Error;

    async fn connect_once(
        &self,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError> {
        connect_to_compute_once(node_info, self.conn_info, timeout, self.session_id).await
    }

    fn update_connect_config(&self, _config: &mut compute::ConnCfg) {}
}

// Wake up the destination if needed. Code here is a bit involved because
// we reuse the code from the usual proxy and we need to prepare few structures
// that this code expects.
#[tracing::instrument(skip_all)]
async fn connect_to_compute(
    config: &config::ProxyConfig,
    conn_info: &ConnInfo,
    session_id: uuid::Uuid,
) -> anyhow::Result<Client> {
    let tls = config.tls_config.as_ref();
    let common_names = tls.and_then(|tls| tls.common_names.clone());

    let credential_params = StartupMessageParams::new([
        ("user", &conn_info.username),
        ("database", &conn_info.dbname),
        ("application_name", APP_NAME),
    ]);

    let creds = config
        .auth_backend
        .as_ref()
        .map(|_| {
            auth::ClientCredentials::parse(
                &credential_params,
                Some(&conn_info.hostname),
                common_names,
            )
        })
        .transpose()?;
    let extra = console::ConsoleReqExtra {
        session_id: uuid::Uuid::new_v4(),
        application_name: Some(APP_NAME),
    };

    let node_info = creds
        .wake_compute(&extra)
        .await?
        .context("missing cache entry from wake_compute")?;

    crate::proxy::connect_to_compute(
        &TokioMechanism {
            conn_info,
            session_id,
        },
        node_info,
        &extra,
        &creds,
    )
    .await
}

async fn connect_to_compute_once(
    node_info: &console::CachedNodeInfo,
    conn_info: &ConnInfo,
    timeout: time::Duration,
    mut session: uuid::Uuid,
) -> Result<Client, tokio_postgres::Error> {
    let mut config = (*node_info.config).clone();

    let (client, mut connection) = config
        .user(&conn_info.username)
        .password(&conn_info.password)
        .dbname(&conn_info.dbname)
        .max_backend_message_size(MAX_RESPONSE_SIZE)
        .connect_timeout(timeout)
        .connect(tokio_postgres::NoTls)
        .await?;

    let (tx, mut rx) = tokio::sync::watch::channel(session);

    let conn_id = uuid::Uuid::new_v4();
    let span = info_span!(parent: None, "connection", %conn_id);
    span.in_scope(|| {
        info!(%conn_info, %session, "new connection");
    });

    tokio::spawn(
        poll_fn(move |cx| {
            if matches!(rx.has_changed(), Ok(true)) {
                session = *rx.borrow_and_update();
                info!(%session, "changed session");
            }

            loop {
                let message = ready!(connection.poll_message(cx));

                match message {
                    Some(Ok(AsyncMessage::Notice(notice))) => {
                        info!(%session, "notice: {}", notice);
                    }
                    Some(Ok(AsyncMessage::Notification(notif))) => {
                        warn!(%session, pid = notif.process_id(), channel = notif.channel(), "notification received");
                    }
                    Some(Ok(_)) => {
                        warn!(%session, "unknown message");
                    }
                    Some(Err(e)) => {
                        error!(%session, "connection error: {}", e);
                        return Poll::Ready(())
                    }
                    None => {
                        info!("connection closed");
                        return Poll::Ready(())
                    }
                }
            }
        })
        .instrument(span)
    );

    Ok(Client {
        inner: client,
        session: tx,
    })
}

pub struct Client {
    pub inner: tokio_postgres::Client,
    session: tokio::sync::watch::Sender<uuid::Uuid>,
}
