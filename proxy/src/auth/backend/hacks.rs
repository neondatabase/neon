use super::{
    ComputeCredentialKeys, ComputeCredentials, ComputeUserInfo, ComputeUserInfoNoEndpoint,
};
use crate::{
    auth::{self, AuthFlow},
    console::AuthSecret,
    metrics::LatencyTimer,
    sasl,
    stream::{self, Stream},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

/// Compared to [SCRAM](crate::scram), cleartext password auth saves
/// one round trip and *expensive* computations (>= 4096 HMAC iterations).
/// These properties are benefical for serverless JS workers, so we
/// use this mechanism for websocket connections.
pub async fn authenticate_cleartext(
    info: ComputeUserInfo,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    latency_timer: &mut LatencyTimer,
    secret: AuthSecret,
) -> auth::Result<ComputeCredentials<ComputeCredentialKeys>> {
    warn!("cleartext auth flow override is enabled, proceeding");

    // pause the timer while we communicate with the client
    let _paused = latency_timer.pause();

    let auth_outcome = AuthFlow::new(client)
        .begin(auth::CleartextPassword(secret))
        .await?
        .authenticate()
        .await?;

    let keys = match auth_outcome {
        sasl::Outcome::Success(key) => key,
        sasl::Outcome::Failure(reason) => {
            info!("auth backend failed with an error: {reason}");
            return Err(auth::AuthError::auth_failed(&*info.inner.user));
        }
    };

    Ok(ComputeCredentials { info, keys })
}

/// Workaround for clients which don't provide an endpoint (project) name.
/// Similar to [`authenticate_cleartext`], but there's a specific password format,
/// and passwords are not yet validated (we don't know how to validate them!)
pub async fn password_hack_no_authentication(
    info: ComputeUserInfoNoEndpoint,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<ComputeCredentials<Vec<u8>>> {
    warn!("project not specified, resorting to the password hack auth flow");

    // pause the timer while we communicate with the client
    let _paused = latency_timer.pause();

    let payload = AuthFlow::new(client)
        .begin(auth::PasswordHack)
        .await?
        .get_password()
        .await?;

    info!(project = &*payload.endpoint, "received missing parameter");

    // Report tentative success; compute node will check the password anyway.
    Ok(ComputeCredentials {
        info: ComputeUserInfo {
            inner: info,
            endpoint: payload.endpoint,
        },
        keys: payload.password,
    })
}
