use std::{borrow::Cow, ops::ControlFlow};

use pq_proto::StartupMessageParams;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, warn};

use crate::{
    auth::{
        backend::{ComputeCredentialKeys, ComputeCredentials},
        BackendType,
    },
    cancellation::Session,
    console::{provider::ConsoleBackend, Api},
    context::RequestMonitoring,
    state_machine::{user_facing_error, DynStage, Stage, StageError},
    stream::{PqStream, Stream},
};

use super::{
    connect_compute::{handle_try_wake, NeedsComputeConnection, TcpMechanism},
    retry::retry_after,
};

pub struct NeedsWakeCompute<S> {
    pub stream: PqStream<Stream<S>>,
    pub api: Cow<'static, ConsoleBackend>,
    pub params: StartupMessageParams,
    pub allow_self_signed_compute: bool,
    pub creds: ComputeCredentials<ComputeCredentialKeys>,

    // monitoring
    pub ctx: RequestMonitoring,
    pub cancel_session: Session,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stage for NeedsWakeCompute<S> {
    fn span(&self) -> tracing::Span {
        tracing::info_span!("wake_compute")
    }
    async fn run(self) -> Result<DynStage, StageError> {
        let Self {
            stream,
            api,
            params,
            allow_self_signed_compute,
            creds,
            mut ctx,
            cancel_session,
        } = self;

        let mut num_retries = 0;
        let mut node_info = loop {
            let wake_res = api.wake_compute(&mut ctx, &creds.info).await;
            match handle_try_wake(wake_res, num_retries) {
                Err(e) => {
                    error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                    return Err(user_facing_error(e, &mut ctx, stream));
                }
                Ok(ControlFlow::Continue(e)) => {
                    warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
                }
                Ok(ControlFlow::Break(n)) => break n,
            }

            let wait_duration = retry_after(num_retries);
            num_retries += 1;
            tokio::time::sleep(wait_duration).await;
        };

        ctx.set_project(node_info.aux.clone());

        node_info.allow_self_signed_compute = allow_self_signed_compute;

        match creds.keys {
            #[cfg(feature = "testing")]
            ComputeCredentialKeys::Password(password) => node_info.config.password(password),
            ComputeCredentialKeys::AuthKeys(auth_keys) => node_info.config.auth_keys(auth_keys),
        };

        Ok(Box::new(NeedsComputeConnection {
            stream,
            user_info: BackendType::Console(api, creds.info),
            mechanism: TcpMechanism { params },
            node_info,
            ctx,
            cancel_session,
        }))
    }
}
