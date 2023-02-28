use super::AuthSuccess;
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    console::{
        self,
        provider::{CachedNodeInfo, ConsoleReqExtra},
    },
    stream::PqStream,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

/// Wake the compute node, but only if the password is valid.
async fn get_compute(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &mut ClientCredentials<'_>,
    password: Vec<u8>,
) -> auth::Result<CachedNodeInfo> {
    // TODO: this will slow down both "hacks" below; we probably need a cache.
    let info = console::get_auth_info(api, extra, creds).await?;

    let secret = info.scram_or_goodbye()?;
    if !secret.matches_password(&password) {
        info!("our obscure magic indicates that the password doesn't match");
        return Err(auth::AuthError::auth_failed(creds.user));
    }

    let mut node = api.wake_compute(extra, creds).await?;
    node.config.password(password);

    Ok(node)
}

/// Compared to [SCRAM](crate::scram), cleartext password auth saves
/// one round trip and *expensive* computations (>= 4096 HMAC iterations).
/// These properties are benefical for serverless JS workers, so we
/// use this mechanism for websocket connections.
pub async fn cleartext_hack(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &mut ClientCredentials<'_>,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    warn!("cleartext auth flow override is enabled, proceeding");
    let password = AuthFlow::new(client)
        .begin(auth::CleartextPassword)
        .await?
        .authenticate()
        .await?;

    let node = get_compute(api, extra, creds, password).await?;

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
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    warn!("project not specified, resorting to the password hack auth flow");
    let payload = AuthFlow::new(client)
        .begin(auth::PasswordHack)
        .await?
        .authenticate()
        .await?;

    info!(project = &payload.project, "received missing parameter");
    creds.project = Some(payload.project.into());

    let node = get_compute(api, extra, creds, payload.password).await?;

    // Report tentative success; compute node will check the password anyway.
    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}
