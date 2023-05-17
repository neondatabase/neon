use super::AuthSuccess;
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    console::{
        self,
        provider::{CachedNodeInfo, ConsoleReqExtra},
    },
    stream,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

/// Compared to [SCRAM](crate::scram), cleartext password auth saves
/// one round trip and *expensive* computations (>= 4096 HMAC iterations).
/// These properties are benefical for serverless JS workers, so we
/// use this mechanism for websocket connections.
pub async fn cleartext_hack(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &mut ClientCredentials<'_>,
    client: &mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    warn!("cleartext auth flow override is enabled, proceeding");
    let password = AuthFlow::new(client)
        .begin(auth::CleartextPassword)
        .await?
        .authenticate()
        .await?;

    let mut node = api.wake_compute(extra, creds).await?;
    node.config.password(password);

    // Report tentative success; compute node will check the password anyway.
    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}

/// Workaround for clients which don't provide an endpoint (project) name.
/// Very similar to [`cleartext_hack`], but there's a specific password format.
pub async fn password_hack(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &mut ClientCredentials<'_>,
    client: &mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    warn!("project not specified, resorting to the password hack auth flow");
    let payload = AuthFlow::new(client)
        .begin(auth::PasswordHack)
        .await?
        .authenticate()
        .await?;

    info!(project = &payload.endpoint, "received missing parameter");
    creds.project = Some(payload.endpoint);

    let mut node = api.wake_compute(extra, creds).await?;
    node.config.password(payload.password);

    // Report tentative success; compute node will check the password anyway.
    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}
