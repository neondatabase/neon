use super::AuthSuccess;
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute,
    console::{self, AuthInfo, CachedNodeInfo, ConsoleReqExtra},
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

    let scram_keys = match info {
        AuthInfo::Md5(_) => {
            info!("auth endpoint chooses MD5");
            return Err(auth::AuthError::bad_auth_method("MD5"));
        }
        AuthInfo::Scram(secret) => {
            info!("auth endpoint chooses SCRAM");
            do_scram(secret, creds, client).await?
        }
    };

    let mut node = api.wake_compute(extra, creds).await?;
    node.config.auth_keys(AuthKeys::ScramSha256(scram_keys));

    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}
