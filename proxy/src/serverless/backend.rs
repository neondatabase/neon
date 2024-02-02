use std::{sync::Arc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use tracing::info;

use crate::{
    auth::{backend::ComputeCredentialKeys, check_peer_addr_is_in_list, AuthError},
    compute,
    config::ProxyConfig,
    console::CachedNodeInfo,
    context::RequestMonitoring,
    proxy::connect_compute::ConnectMechanism,
};

use super::conn_pool::{Client, ConnInfo, GlobalConnPool, APP_NAME};

pub struct PoolingBackend {
    pub pool: Arc<GlobalConnPool>,
    pub config: &'static ProxyConfig,
}

impl PoolingBackend {
    pub async fn authenticate(
        &self,
        ctx: &mut RequestMonitoring,
        conn_info: &ConnInfo,
    ) -> Result<ComputeCredentialKeys, AuthError> {
        let user_info = conn_info.user_info.clone();
        let backend = self.config.auth_backend.as_ref().map(|_| user_info.clone());
        let (allowed_ips, maybe_secret) = backend.get_allowed_ips_and_secret(ctx).await?;
        if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
            return Err(AuthError::ip_address_not_allowed());
        }
        let cached_secret = match maybe_secret {
            Some(secret) => secret,
            None => backend.get_role_secret(ctx).await?,
        };

        let secret = match cached_secret.value.clone() {
            Some(secret) => secret,
            None => {
                // If we don't have an authentication secret, for the http flow we can just return an error.
                info!("authentication info not found");
                return Err(AuthError::auth_failed(&*user_info.user));
            }
        };
        let auth_outcome =
            crate::auth::validate_password_and_exchange(conn_info.password.as_bytes(), secret)?;
        match auth_outcome {
            crate::sasl::Outcome::Success(key) => Ok(key),
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                Err(AuthError::auth_failed(&*conn_info.user_info.user))
            }
        }
    }

    // Wake up the destination if needed. Code here is a bit involved because
    // we reuse the code from the usual proxy and we need to prepare few structures
    // that this code expects.
    #[tracing::instrument(fields(pid = tracing::field::Empty), skip_all)]
    pub async fn connect_to_compute(
        &self,
        ctx: &mut RequestMonitoring,
        conn_info: ConnInfo,
        keys: ComputeCredentialKeys,
        force_new: bool,
    ) -> anyhow::Result<Client> {
        let maybe_client = if !force_new {
            info!("pool: looking for an existing connection");
            self.pool.get(ctx, &conn_info).await?
        } else {
            info!("pool: pool is disabled");
            None
        };

        if let Some(client) = maybe_client {
            return Ok(client);
        }
        let conn_id = uuid::Uuid::new_v4();
        info!(%conn_id, "pool: opening a new connection '{conn_info}'");
        ctx.set_application(Some(APP_NAME));
        let backend = self
            .config
            .auth_backend
            .as_ref()
            .map(|_| conn_info.user_info.clone());

        let mut node_info = backend
            .wake_compute(ctx)
            .await?
            .context("missing cache entry from wake_compute")?;

        match keys {
            #[cfg(feature = "testing")]
            ComputeCredentialKeys::Password(password) => node_info.config.password(password),
            ComputeCredentialKeys::AuthKeys(auth_keys) => node_info.config.auth_keys(auth_keys),
        };

        ctx.set_project(node_info.aux.clone());

        crate::proxy::connect_compute::connect_to_compute(
            ctx,
            &TokioMechanism {
                conn_id,
                conn_info,
                pool: self.pool.clone(),
            },
            node_info,
            &backend,
        )
        .await
    }
}

struct TokioMechanism {
    pool: Arc<GlobalConnPool>,
    conn_info: ConnInfo,
    conn_id: uuid::Uuid,
}

#[async_trait]
impl ConnectMechanism for TokioMechanism {
    type Connection = Client;
    type ConnectError = tokio_postgres::Error;
    type Error = anyhow::Error;

    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        node_info: &CachedNodeInfo,
        timeout: Duration,
    ) -> Result<Self::Connection, Self::ConnectError> {
        let mut config = (*node_info.config).clone();
        let config = config
            .user(&self.conn_info.user_info.user)
            .password(&*self.conn_info.password)
            .dbname(&self.conn_info.dbname)
            .connect_timeout(timeout);

        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;

        tracing::Span::current().record("pid", &tracing::field::display(client.get_process_id()));
        let pool = self.pool.clone();
        Ok(pool.poll_client(
            ctx,
            self.conn_info.clone(),
            client,
            connection,
            self.conn_id,
            node_info.aux.clone(),
        ))
    }

    fn update_connect_config(&self, _config: &mut compute::ConnCfg) {}
}
