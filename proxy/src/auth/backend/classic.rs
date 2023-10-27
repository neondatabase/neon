use super::{AuthSuccess, ComputeCredentials};
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute,
    config::AuthenticationConfig,
    console::{self, AuthInfo, ConsoleReqExtra},
    proxy::LatencyTimer,
    sasl, scram,
    stream::PqStream,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

pub(super) async fn authenticate(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &ClientCredentials<'_>,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    config: &'static AuthenticationConfig,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<AuthSuccess<ComputeCredentials>> {
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

            let auth_outcome = tokio::time::timeout(
                config.scram_protocol_timeout,
                async {
                    // pause the timer while we communicate with the client
                    let _paused = latency_timer.pause();

                    flow.begin(scram).await.map_err(|error| {
                        warn!(?error, "error sending scram acknowledgement");
                        error
                    })?.authenticate().await.map_err(|error| {
                        warn!(?error, "error processing scram messages");
                        error
                    })
                }
            )
            .await
            .map_err(|error| {
                warn!("error processing scram messages error = authentication timed out, execution time exeeded {} seconds", config.scram_protocol_timeout.as_secs());
                auth::io::Error::new(auth::io::ErrorKind::TimedOut, error)
            })??;

            let client_key = match auth_outcome {
                sasl::Outcome::Success(key) => key,
                sasl::Outcome::Failure(reason) => {
                    info!("auth backend failed with an error: {reason}");
                    return Err(auth::AuthError::auth_failed(creds.user));
                }
            };

            compute::ScramKeys {
                client_key: client_key.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            }
        }
    };

    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: ComputeCredentials::AuthKeys(tokio_postgres::config::AuthKeys::ScramSha256(
            scram_keys,
        )),
    })
}
