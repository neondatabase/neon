use super::{ComputeCredentials, ComputeUserInfo, ComputeUserInfoNoEndpoint};
use crate::{
    auth::{self, AuthFlow},
    proxy::LatencyTimer,
    stream::{self, Stream},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

/// Compared to [SCRAM](crate::scram), cleartext password auth saves
/// one round trip and *expensive* computations (>= 4096 HMAC iterations).
/// These properties are benefical for serverless JS workers, so we
/// use this mechanism for websocket connections.
pub async fn cleartext_hack<'a>(
    info: ComputeUserInfo<'a>,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<ComputeCredentials<'a, Vec<u8>>> {
    warn!("cleartext auth flow override is enabled, proceeding");

    // pause the timer while we communicate with the client
    let _paused = latency_timer.pause();

    let password = AuthFlow::new(client)
        .begin(auth::CleartextPassword)
        .await?
        .authenticate()
        .await?;

    // Report tentative success; compute node will check the password anyway.
    Ok(ComputeCredentials {
        info,
        keys: password,
    })
}

/// Workaround for clients which don't provide an endpoint (project) name.
/// Very similar to [`cleartext_hack`], but there's a specific password format.
pub async fn password_hack<'a>(
    info: ComputeUserInfoNoEndpoint<'a>,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<ComputeCredentials<'a, Vec<u8>>> {
    warn!("project not specified, resorting to the password hack auth flow");

    // pause the timer while we communicate with the client
    let _paused = latency_timer.pause();

    let payload = AuthFlow::new(client)
        .begin(auth::PasswordHack)
        .await?
        .authenticate()
        .await?;

    info!(project = &payload.endpoint, "received missing parameter");

    // Report tentative success; compute node will check the password anyway.
    Ok(ComputeCredentials {
        info: ComputeUserInfo {
            inner: info,
            endpoint: payload.endpoint,
        },
        keys: payload.password,
    })
}
