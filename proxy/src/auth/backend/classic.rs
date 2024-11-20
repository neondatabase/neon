use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, warn};

use super::{ComputeCredentials, ComputeUserInfo};
use crate::auth::backend::ComputeCredentialKeys;
use crate::auth::{self, AuthFlow};
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::control_plane::AuthSecret;
use crate::stream::{PqStream, Stream};
use crate::{compute, sasl};

pub(super) async fn authenticate(
    ctx: &RequestContext,
    creds: ComputeUserInfo,
    client: &mut PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    config: &'static AuthenticationConfig,
    secret: AuthSecret,
) -> auth::Result<ComputeCredentials> {
    let flow = AuthFlow::new(client);
    let scram_keys = match secret {
        #[cfg(any(test, feature = "testing"))]
        AuthSecret::Md5(_) => {
            debug!("auth endpoint chooses MD5");
            return Err(auth::AuthError::bad_auth_method("MD5"));
        }
        AuthSecret::Scram(secret) => {
            debug!("auth endpoint chooses SCRAM");
            let scram = auth::Scram(&secret, ctx);

            let auth_outcome = tokio::time::timeout(
                config.scram_protocol_timeout,
                async {

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
            .map_err(|e| {
                warn!("error processing scram messages error = authentication timed out, execution time exceeded {} seconds", config.scram_protocol_timeout.as_secs());
                auth::AuthError::user_timeout(e)
            })??;

            let client_key = match auth_outcome {
                sasl::Outcome::Success(key) => key,
                sasl::Outcome::Failure(reason) => {
                    // TODO: warnings?
                    // TODO: should we get rid of this because double logging?
                    info!("auth backend failed with an error: {reason}");
                    return Err(auth::AuthError::password_failed(&*creds.user));
                }
            };

            compute::ScramKeys {
                client_key: client_key.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            }
        }
    };

    Ok(ComputeCredentials {
        info: creds,
        keys: ComputeCredentialKeys::AuthKeys(tokio_postgres::config::AuthKeys::ScramSha256(
            scram_keys,
        )),
    })
}
