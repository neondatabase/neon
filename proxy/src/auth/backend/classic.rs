use super::AuthSuccess;
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute,
    console::{self, CachedNodeInfo, ConsoleReqExtra},
    sasl, scram,
    stream::PqStream,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::AuthKeys;
use tracing::info;

async fn do_scram(
    secret: scram::ServerSecret,
    creds: &ClientCredentials<'_>,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<compute::ScramKeys> {
    let outcome = AuthFlow::new(client)
        .begin(auth::Scram(&secret))
        .await?
        .authenticate()
        .await?;

    let client_key = match outcome {
        sasl::Outcome::Success(key) => key,
        sasl::Outcome::Failure(reason) => {
            info!("auth backend failed with an error: {reason}");
            return Err(auth::AuthError::auth_failed(creds.user));
        }
    };

    let keys = compute::ScramKeys {
        client_key: client_key.as_bytes(),
        server_key: secret.server_key.as_bytes(),
    };

    Ok(keys)
}

pub async fn authenticate(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &ClientCredentials<'_>,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    let info = console::get_auth_info(api, extra, creds).await?;

    let secret = info.scram_or_goodbye()?;
    let scram_keys = do_scram(secret, creds, client).await?;

    let mut node = api.wake_compute(extra, creds).await?;
    node.config.auth_keys(AuthKeys::ScramSha256(scram_keys));

    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}
