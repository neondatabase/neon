use super::{AuthSuccess, ComputeCredentials};
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute,
    config::AuthenticationConfig,
    console::AuthSecret,
    proxy::LatencyTimer,
    sasl,
    stream::{PqStream, Stream},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

pub(super) async fn authenticate(
    creds: &ClientCredentials<'_>,
    client: &mut PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    config: &'static AuthenticationConfig,
    latency_timer: &mut LatencyTimer,
    secret: AuthSecret,
) -> auth::Result<AuthSuccess<ComputeCredentials>> {
    let flow = AuthFlow::new(client);
    let scram_keys = match secret {
        AuthSecret::Md5(_) => {
            info!("auth endpoint chooses MD5");
            return Err(auth::AuthError::bad_auth_method("MD5"));
        }
        AuthSecret::Scram(secret) => {
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
