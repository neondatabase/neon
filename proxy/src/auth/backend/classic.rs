use std::ops::ControlFlow;

use super::AuthSuccess;
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute,
    console::{self, AuthInfo, CachedNodeInfo, ConsoleReqExtra},
    proxy::{handle_try_wake, retry_after},
    sasl, scram,
    stream::PqStream,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, info, warn};

pub(super) async fn authenticate(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &ClientCredentials<'_>,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    info!("fetching user's authentication info");
    let info = api.get_auth_info(extra, creds).await?.unwrap_or_else(|| {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        info!("authentication info not found, mocking it");
        AuthInfo::Scram(scram::ServerSecret::mock(creds.user, rand::random()))
    });

    let flow = AuthFlow::new(client);
    let scram_keys = match info {
        AuthInfo::Md5(_) => {
            info!("auth endpoint chooses MD5");
            return Err(auth::AuthError::bad_auth_method("MD5"));
        }
        AuthInfo::Scram(secret) => {
            info!("auth endpoint chooses SCRAM");
            let scram = auth::Scram(&secret);

            let auth_flow = flow.begin(scram).await.map_err(|error| {
                warn!(?error, "error sending scram acknowledgement");
                error
            })?;

            let auth_outcome = auth_flow.authenticate().await.map_err(|error| {
                warn!(?error, "error processing scram messages");
                error
            })?;

            let client_key = match auth_outcome {
                sasl::Outcome::Success(key) => key,
                sasl::Outcome::Failure(reason) => {
                    info!("auth backend failed with an error: {reason}");
                    return Err(auth::AuthError::auth_failed(creds.user));
                }
            };

            Some(compute::ScramKeys {
                client_key: client_key.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            })
        }
    };

    let mut num_retries = 0;
    let mut node = loop {
        let wake_res = api.wake_compute(extra, creds).await;
        match handle_try_wake(wake_res, num_retries) {
            Err(e) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                return Err(e.into());
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
    if let Some(keys) = scram_keys {
        use tokio_postgres::config::AuthKeys;
        node.config.auth_keys(AuthKeys::ScramSha256(keys));
    }

    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}
