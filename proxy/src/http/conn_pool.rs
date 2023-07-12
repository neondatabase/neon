use parking_lot::Mutex;
use pq_proto::StartupMessageParams;
use std::fmt;
use std::ops::ControlFlow;
use std::{collections::HashMap, sync::Arc};
use tokio::time;

use crate::config;
use crate::{auth, console};

use super::sql_over_http::MAX_RESPONSE_SIZE;

use crate::proxy::{invalidate_cache, retry_after, try_wake, NUM_RETRIES_WAKE_COMPUTE};

use tracing::error;
use tracing::info;

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
    conn: tokio_postgres::Client,
    _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> Vec<ConnPoolEntry>
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub struct EndpointConnPool {
    pools: HashMap<(String, String), Vec<ConnPoolEntry>>,
    total_conns: usize,
}

pub struct GlobalConnPool {
    // endpoint -> per-endpoint connection pool
    //
    // That should be a fairly conteded map, so return reference to the per-endpoint
    // pool as early as possible and release the lock.
    global_pool: Mutex<HashMap<String, Arc<Mutex<EndpointConnPool>>>>,

    // Maximum number of connections per one endpoint.
    // Can mix different (dbname, username) connections.
    // When running out of free slots for a particular endpoint,
    // falls back to opening a new connection for each request.
    max_conns_per_endpoint: usize,

    proxy_config: &'static crate::config::ProxyConfig,
}

impl GlobalConnPool {
    pub fn new(config: &'static crate::config::ProxyConfig) -> Arc<Self> {
        Arc::new(Self {
            global_pool: Mutex::new(HashMap::new()),
            max_conns_per_endpoint: MAX_CONNS_PER_ENDPOINT,
            proxy_config: config,
        })
    }

    pub async fn get(
        &self,
        conn_info: &ConnInfo,
        force_new: bool,
    ) -> anyhow::Result<tokio_postgres::Client> {
        let mut client: Option<tokio_postgres::Client> = None;

        if !force_new {
            let pool = self.get_endpoint_pool(&conn_info.hostname).await;

            // find a pool entry by (dbname, username) if exists
            let mut pool = pool.lock();
            let pool_entries = pool.pools.get_mut(&conn_info.db_and_user());
            if let Some(pool_entries) = pool_entries {
                if let Some(entry) = pool_entries.pop() {
                    client = Some(entry.conn);
                    pool.total_conns -= 1;
                }
            }
        }

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            if client.is_closed() {
                info!("pool: cached connection '{conn_info}' is closed, opening a new one");
                connect_to_compute(self.proxy_config, conn_info).await
            } else {
                info!("pool: reusing connection '{conn_info}'");
                Ok(client)
            }
        } else {
            info!("pool: opening a new connection '{conn_info}'");
            connect_to_compute(self.proxy_config, conn_info).await
        }
    }

    pub async fn put(
        &self,
        conn_info: &ConnInfo,
        client: tokio_postgres::Client,
    ) -> anyhow::Result<()> {
        let pool = self.get_endpoint_pool(&conn_info.hostname).await;

        // return connection to the pool
        let mut total_conns;
        let mut returned = false;
        let mut per_db_size = 0;
        {
            let mut pool = pool.lock();
            total_conns = pool.total_conns;

            let pool_entries: &mut Vec<ConnPoolEntry> = pool
                .pools
                .entry(conn_info.db_and_user())
                .or_insert_with(|| Vec::with_capacity(1));
            if total_conns < self.max_conns_per_endpoint {
                pool_entries.push(ConnPoolEntry {
                    conn: client,
                    _last_access: std::time::Instant::now(),
                });

                total_conns += 1;
                returned = true;
                per_db_size = pool_entries.len();

                pool.total_conns += 1;
            }
        }

        // do logging outside of the mutex
        if returned {
            info!("pool: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!("pool: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }

        Ok(())
    }

    async fn get_endpoint_pool(&self, endpoint: &String) -> Arc<Mutex<EndpointConnPool>> {
        // find or create a pool for this endpoint
        let mut created = false;
        let mut global_pool = self.global_pool.lock();
        let pool = global_pool
            .entry(endpoint.clone())
            .or_insert_with(|| {
                created = true;
                Arc::new(Mutex::new(EndpointConnPool {
                    pools: HashMap::new(),
                    total_conns: 0,
                }))
            })
            .clone();
        let global_pool_size = global_pool.len();
        drop(global_pool);

        // log new global pool size
        if created {
            info!(
                "pool: created new pool for '{endpoint}', global pool size now {global_pool_size}"
            );
        }

        pool
    }
}

// Wake up the destination if needed. Code here is a bit involved because
// we reuse the code from the usual proxy and we need to prepare few structures
// that this code expects.
#[tracing::instrument(skip_all)]
async fn connect_to_compute(
    config: &config::ProxyConfig,
    conn_info: &ConnInfo,
) -> anyhow::Result<tokio_postgres::Client> {
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

    let node_info = &mut creds.wake_compute(&extra).await?.expect("msg");

    let mut num_retries = 0;
    let mut wait_duration = time::Duration::ZERO;
    let mut should_wake_with_error = None;
    loop {
        if !wait_duration.is_zero() {
            time::sleep(wait_duration).await;
        }

        // try wake the compute node if we have determined it's sensible to do so
        if let Some(err) = should_wake_with_error.take() {
            match try_wake(node_info, &extra, &creds).await {
                // we can't wake up the compute node
                Ok(None) => return Err(err),
                // there was an error communicating with the control plane
                Err(e) => return Err(e.into()),
                // failed to wake up but we can continue to retry
                Ok(Some(ControlFlow::Continue(()))) => {
                    wait_duration = retry_after(num_retries);
                    should_wake_with_error = Some(err);

                    num_retries += 1;
                    info!(num_retries, "retrying wake compute");
                    continue;
                }
                // successfully woke up a compute node and can break the wakeup loop
                Ok(Some(ControlFlow::Break(()))) => {}
            }
        }

        match connect_to_compute_once(node_info, conn_info).await {
            Ok(res) => return Ok(res),
            Err(e) => {
                error!(error = ?e, "could not connect to compute node");
                if !can_retry_error(&e, num_retries) {
                    return Err(e.into());
                }
                wait_duration = retry_after(num_retries);

                // after the first connect failure,
                // we should invalidate the cache and wake up a new compute node
                if num_retries == 0 {
                    invalidate_cache(node_info);
                    should_wake_with_error = Some(e.into());
                }
            }
        }

        num_retries += 1;
        info!(num_retries, "retrying connect");
    }
}

fn can_retry_error(err: &tokio_postgres::Error, num_retries: u32) -> bool {
    use tokio_postgres::error::SqlState;
    match err.code() {
        // retry all errors at least once
        _ if num_retries == 0 => true,
        // keep retrying connection errors
        Some(
            &SqlState::CONNECTION_FAILURE
            | &SqlState::CONNECTION_EXCEPTION
            | &SqlState::CONNECTION_DOES_NOT_EXIST
            | &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        ) if num_retries < NUM_RETRIES_WAKE_COMPUTE => true,
        // otherwise, don't retry
        _ => false,
    }
}

async fn connect_to_compute_once(
    node_info: &console::CachedNodeInfo,
    conn_info: &ConnInfo,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut config = (*node_info.config).clone();

    let (client, connection) = config
        .user(&conn_info.username)
        .password(&conn_info.password)
        .dbname(&conn_info.dbname)
        .max_backend_message_size(MAX_RESPONSE_SIZE)
        .connect(tokio_postgres::NoTls)
        .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    Ok(client)
}
