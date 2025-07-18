#[cfg(test)]
mod tests;

pub(crate) mod connect_compute;
pub(crate) mod retry;
pub(crate) mod wake_compute;

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;

use itertools::Itertools;
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::cache::Cache;
use crate::cancellation::CancellationHandler;
use crate::compute::ComputeConnection;
use crate::config::ProxyConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ControlPlaneClient;
pub use crate::pglb::copy_bidirectional::{ErrorSource, copy_bidirectional_client_compute};
use crate::pglb::{ClientMode, ClientRequestError};
use crate::pqproto::{BeMessage, CancelKeyData, StartupMessageParams};
use crate::proxy::connect_compute::{TcpMechanism, connect_to_compute};
use crate::proxy::retry::ShouldRetryWakeCompute;
use crate::rate_limiter::EndpointRateLimiter;
use crate::stream::{PqStream, Stream};
use crate::types::EndpointCacheKey;
use crate::{auth, compute};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_client<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    client: &mut PqStream<Stream<S>>,
    mode: &ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    common_names: Option<&HashSet<String>>,
    params: &StartupMessageParams,
) -> Result<(ComputeConnection, oneshot::Sender<Infallible>), ClientRequestError> {
    let hostname = mode.hostname(client.get_ref());
    // Extract credentials which we're going to use for auth.
    let result = auth_backend
        .as_ref()
        .map(|()| auth::ComputeUserInfoMaybeEndpoint::parse(ctx, params, hostname, common_names))
        .transpose();

    let user_info = match result {
        Ok(user_info) => user_info,
        Err(e) => Err(client.throw_error(e, Some(ctx)).await)?,
    };

    let user = user_info.get_user().to_owned();
    let user_info = match user_info
        .authenticate(
            ctx,
            client,
            mode.allow_cleartext(),
            &config.authentication_config,
            endpoint_rate_limiter,
        )
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => {
            let db = params.get("database");
            let app = params.get("application_name");
            let params_span = tracing::info_span!("", ?user, ?db, ?app);

            return Err(client
                .throw_error(e, Some(ctx))
                .instrument(params_span)
                .await)?;
        }
    };

    let (cplane, creds) = match user_info {
        auth::Backend::ControlPlane(cplane, creds) => (cplane, creds),
        auth::Backend::Local(_) => unreachable!("local proxy does not run tcp proxy service"),
    };
    let params_compat = creds.info.options.get(NeonOptions::PARAMS_COMPAT).is_some();
    let mut auth_info = compute::AuthInfo::with_auth_keys(creds.keys);
    auth_info.set_startup_params(params, params_compat);

    let mut node;
    let mut attempt = 0;
    let connect = TcpMechanism {
        locks: &config.connect_compute_locks,
    };
    let backend = auth::Backend::ControlPlane(cplane, creds.info);

    // NOTE: This is messy, but should hopefully be detangled with PGLB.
    // We wanted to separate the concerns of **connect** to compute (a PGLB operation),
    // from **authenticate** to compute (a NeonKeeper operation).
    //
    // This unfortunately removed retry handling for one error case where
    // the compute was cached, and we connected, but the compute cache was actually stale
    // and is associated with the wrong endpoint. We detect this when the **authentication** fails.
    // As such, we retry once here if the `authenticate` function fails and the error is valid to retry.
    let pg_settings = loop {
        attempt += 1;

        // TODO: callback to pglb
        let res = connect_to_compute(
            ctx,
            &connect,
            &backend,
            config.wake_compute_retry_config,
            &config.connect_to_compute,
        )
        .await;

        match res {
            Ok(n) => node = n,
            Err(e) => return Err(client.throw_error(e, Some(ctx)).await)?,
        }

        let auth::Backend::ControlPlane(cplane, user_info) = &backend else {
            unreachable!("ensured above");
        };

        let res = auth_info.authenticate(ctx, &mut node, user_info).await;
        match res {
            Ok(pg_settings) => break pg_settings,
            Err(e) if attempt < 2 && e.should_retry_wake_compute() => {
                tracing::warn!(error = ?e, "retrying wake compute");

                #[allow(irrefutable_let_patterns)]
                if let ControlPlaneClient::ProxyV1(cplane_proxy_v1) = &**cplane {
                    let key = user_info.endpoint_cache_key();
                    cplane_proxy_v1.caches.node_info.invalidate(&key);
                }
            }
            Err(e) => Err(client.throw_error(e, Some(ctx)).await)?,
        }
    };

    let session = cancellation_handler.get_key();

    finish_client_init(ctx, &pg_settings, *session.key(), client);

    let session_id = ctx.session_id();
    let (cancel_on_shutdown, cancel) = oneshot::channel();
    tokio::spawn(async move {
        session
            .maintain_cancel_key(
                session_id,
                cancel,
                &pg_settings.cancel_closure,
                &config.connect_to_compute,
            )
            .await;
    });

    Ok((node, cancel_on_shutdown))
}

/// Finish client connection initialization: confirm auth success, send params, etc.
pub(crate) fn finish_client_init(
    ctx: &RequestContext,
    settings: &compute::PostgresSettings,
    cancel_key_data: CancelKeyData,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) {
    // Forward all deferred notices to the client.
    for notice in &settings.delayed_notice {
        client.write_raw(notice.as_bytes().len(), b'N', |buf| {
            buf.extend_from_slice(notice.as_bytes());
        });
    }

    // Expose session_id to clients
    client.write_message(BeMessage::NoticeResponse(&ctx.session_id().to_string()));

    // Forward all postgres connection params to the client.
    for (name, value) in &settings.params {
        client.write_message(BeMessage::ParameterStatus {
            name: name.as_bytes(),
            value: value.as_bytes(),
        });
    }

    // Forward recorded latencies for probing requests
    if let Some(testodrome_id) = ctx.get_testodrome_id() {
        client.write_message(BeMessage::ParameterStatus {
            name: "testodrome_id".as_bytes(),
            value: testodrome_id.as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "cplane_latency".as_bytes(),
            value: ctx.get_proxy_latency().cplane.as_micros().to_string().as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "client_latency".as_bytes(),
            value: ctx.get_proxy_latency().client.as_micros().to_string().as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "compute_latency".as_bytes(),
            value: ctx.get_proxy_latency().compute.as_micros().to_string().as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "retry_latency".as_bytes(),
            value: ctx.get_proxy_latency().retry.as_micros().to_string().as_bytes(),
        });
    }

    client.write_message(BeMessage::BackendKeyData(cancel_key_data));
    client.write_message(BeMessage::ReadyForQuery);
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct NeonOptions(Vec<(SmolStr, SmolStr)>);

impl NeonOptions {
    // proxy options:

    /// `PARAMS_COMPAT` allows opting in to forwarding all startup parameters from client to compute.
    pub const PARAMS_COMPAT: &'static str = "proxy_params_compat";

    // cplane options:

    /// `LSN` allows provisioning an ephemeral compute with time-travel to the provided LSN.
    const LSN: &'static str = "lsn";

    /// `TIMESTAMP` allows provisioning an ephemeral compute with time-travel to the provided timestamp.
    const TIMESTAMP: &'static str = "timestamp";

    /// `ENDPOINT_TYPE` allows configuring an ephemeral compute to be read_only or read_write.
    const ENDPOINT_TYPE: &'static str = "endpoint_type";

    pub(crate) fn parse_params(params: &StartupMessageParams) -> Self {
        params
            .options_raw()
            .map(Self::parse_from_iter)
            .unwrap_or_default()
    }

    pub(crate) fn parse_options_raw(options: &str) -> Self {
        Self::parse_from_iter(StartupMessageParams::parse_options_raw(options))
    }

    pub(crate) fn get(&self, key: &str) -> Option<SmolStr> {
        self.0
            .iter()
            .find_map(|(k, v)| (k == key).then_some(v))
            .cloned()
    }

    pub(crate) fn is_ephemeral(&self) -> bool {
        self.0.iter().any(|(k, _)| match &**k {
            // This is not a cplane option, we know it does not create ephemeral computes.
            Self::PARAMS_COMPAT => false,
            Self::LSN => true,
            Self::TIMESTAMP => true,
            Self::ENDPOINT_TYPE => true,
            // err on the side of caution. any cplane options we don't know about
            // might lead to ephemeral computes.
            _ => true,
        })
    }

    fn parse_from_iter<'a>(options: impl Iterator<Item = &'a str>) -> Self {
        let mut options = options
            .filter_map(neon_option)
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec();
        options.sort();
        Self(options)
    }

    pub(crate) fn get_cache_key(&self, prefix: &str) -> EndpointCacheKey {
        // prefix + format!(" {k}:{v}")
        // kinda jank because SmolStr is immutable
        std::iter::once(prefix)
            .chain(self.0.iter().flat_map(|(k, v)| [" ", &**k, ":", &**v]))
            .collect::<SmolStr>()
            .into()
    }

    /// <https://swagger.io/docs/specification/serialization/> DeepObject format
    /// `paramName[prop1]=value1&paramName[prop2]=value2&...`
    pub(crate) fn to_deep_object(&self) -> Vec<(SmolStr, SmolStr)> {
        self.0
            .iter()
            .map(|(k, v)| (format_smolstr!("options[{}]", k), v.clone()))
            .collect()
    }
}

pub(crate) fn neon_option(bytes: &str) -> Option<(&str, &str)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").expect("regex should be correct"));

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k, v))
}
