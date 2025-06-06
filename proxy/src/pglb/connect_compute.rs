use async_trait::async_trait;

use crate::auth::backend::ComputeUserInfo;
use crate::compute::{self, AuthInfo, PostgresConnection};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::locks::ApiLocks;
use crate::control_plane::{self, CachedNodeInfo};
use crate::error::ReportableError;
use crate::types::Host;

#[async_trait]
pub(crate) trait ConnectMechanism {
    type Connection;
    type ConnectError: ReportableError;
    type Error: From<Self::ConnectError>;
    async fn connect_once(
        &self,
        ctx: &RequestContext,
        node_info: &control_plane::CachedNodeInfo,
        config: &ComputeConfig,
    ) -> Result<Self::Connection, Self::ConnectError>;
}

#[async_trait]
pub(crate) trait ComputeConnectBackend {
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError>;
}

pub(crate) struct TcpMechanism {
    pub(crate) auth: AuthInfo,
    /// connect_to_compute concurrency lock
    pub(crate) locks: &'static ApiLocks<Host>,
    pub(crate) user_info: ComputeUserInfo,
}

#[async_trait]
impl ConnectMechanism for TcpMechanism {
    type Connection = PostgresConnection;
    type ConnectError = compute::ConnectionError;
    type Error = compute::ConnectionError;

    #[tracing::instrument(skip_all, fields(
        pid = tracing::field::Empty,
        compute_id = tracing::field::Empty
    ))]
    async fn connect_once(
        &self,
        ctx: &RequestContext,
        node_info: &control_plane::CachedNodeInfo,
        config: &ComputeConfig,
    ) -> Result<PostgresConnection, Self::Error> {
        let permit = self.locks.get_permit(&node_info.conn_info.host).await?;
        permit.release_result(
            node_info
                .connect(ctx, &self.auth, config, self.user_info.clone())
                .await,
        )
    }
}
