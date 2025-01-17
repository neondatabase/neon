use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info};

use super::{ComputeCredentials, ComputeUserInfo, ComputeUserInfoNoEndpoint};
use crate::auth::{self, AuthFlow};
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::control_plane::AuthSecret;
use crate::intern::EndpointIdInt;
use crate::sasl;
use crate::stream::{self, Stream};

/// Compared to [SCRAM](crate::scram), cleartext password auth saves
/// one round trip and *expensive* computations (>= 4096 HMAC iterations).
/// These properties are benefical for serverless JS workers, so we
/// use this mechanism for websocket connections.
pub(crate) async fn authenticate_cleartext(
    ctx: &RequestContext,
    info: ComputeUserInfo,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    secret: AuthSecret,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials> {
    debug!("cleartext auth flow override is enabled, proceeding");
    ctx.set_auth_method(crate::context::AuthMethod::Cleartext);

    // pause the timer while we communicate with the client
    let paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

    let ep = EndpointIdInt::from(&info.endpoint);

    let auth_flow = AuthFlow::new(client)
        .begin(auth::CleartextPassword {
            secret,
            endpoint: ep,
            pool: config.thread_pool.clone(),
        })
        .await?;
    drop(paused);
    // cleartext auth is only allowed to the ws/http protocol.
    // If we're here, we already received the password in the first message.
    // Scram protocol will be executed on the proxy side.
    let auth_outcome = auth_flow.authenticate().await?;

    let keys = match auth_outcome {
        sasl::Outcome::Success(key) => key,
        sasl::Outcome::Failure(reason) => {
            info!("auth backend failed with an error: {reason}");
            return Err(auth::AuthError::password_failed(&*info.user));
        }
    };

    Ok(ComputeCredentials { info, keys })
}

/// Workaround for clients which don't provide an endpoint (project) name.
/// Similar to [`authenticate_cleartext`], but there's a specific password format,
/// and passwords are not yet validated (we don't know how to validate them!)
pub(crate) async fn password_hack_no_authentication(
    ctx: &RequestContext,
    info: ComputeUserInfoNoEndpoint,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
) -> auth::Result<(ComputeUserInfo, Vec<u8>)> {
    debug!("project not specified, resorting to the password hack auth flow");
    ctx.set_auth_method(crate::context::AuthMethod::Cleartext);

    // pause the timer while we communicate with the client
    let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

    let payload = AuthFlow::new(client)
        .begin(auth::PasswordHack)
        .await?
        .get_password()
        .await?;

    debug!(project = &*payload.endpoint, "received missing parameter");

    // Report tentative success; compute node will check the password anyway.
    Ok((
        ComputeUserInfo {
            user: info.user,
            options: info.options,
            endpoint: payload.endpoint,
        },
        payload.password,
    ))
}
