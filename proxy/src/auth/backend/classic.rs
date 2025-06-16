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
    let scram_keys = match secret {
        AuthSecret::Scram(secret) => {
            debug!("auth endpoint chooses SCRAM");

            let auth_outcome = tokio::time::timeout(
                config.scram_protocol_timeout,
                AuthFlow::new(client, auth::Scram(&secret, ctx)).authenticate(),
            )
            .await
            .inspect_err(|_| warn!("error processing scram messages error = authentication timed out, execution time exceeded {} seconds", config.scram_protocol_timeout.as_secs()))
            .map_err(auth::AuthError::user_timeout)?
            .inspect_err(|error| warn!(?error, "error processing scram messages"))?;

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
        keys: ComputeCredentialKeys::AuthKeys(postgres_client::config::AuthKeys::ScramSha256(
            scram_keys,
        )),
    })
}
