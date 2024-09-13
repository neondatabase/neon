mod classic;
mod hacks;

use tracing::info;

use crate::auth::backend::{
    ComputeCredentialKeys, ComputeCredentials, ComputeUserInfo, ComputeUserInfoNoEndpoint,
};
use crate::auth::{self, ComputeUserInfoMaybeEndpoint};
use crate::auth_proxy::validate_password_and_exchange;
use crate::console::provider::ConsoleBackend;
use crate::console::AuthSecret;
use crate::context::RequestMonitoring;
use crate::intern::EndpointIdInt;
use crate::proxy::connect_compute::ComputeConnectBackend;
use crate::scram;
use crate::stream::AuthProxyStreamExt;
use crate::{
    config::AuthenticationConfig,
    console::{self, provider::CachedNodeInfo, Api},
};

use super::AuthProxyStream;

/// Alternative to [`std::borrow::Cow`] but doesn't need `T: ToOwned` as we don't need that functionality
pub enum MaybeOwned<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<T> std::ops::Deref for MaybeOwned<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeOwned::Owned(t) => t,
            MaybeOwned::Borrowed(t) => t,
        }
    }
}

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ComputeUserInfoMaybeEndpoint`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
pub enum Backend<'a, T> {
    /// Cloud API (V2).
    Console(MaybeOwned<'a, ConsoleBackend>, T),
}

impl std::fmt::Display for Backend<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Console(api, ()) => match &**api {
                ConsoleBackend::Console(endpoint) => {
                    fmt.debug_tuple("Console").field(&endpoint.url()).finish()
                }
                #[cfg(any(test, feature = "testing"))]
                ConsoleBackend::Postgres(endpoint) => {
                    fmt.debug_tuple("Postgres").field(&endpoint.url()).finish()
                }
                #[cfg(test)]
                ConsoleBackend::Test(_) => fmt.debug_tuple("Test").finish(),
            },
        }
    }
}

impl<T> Backend<'_, T> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub fn as_ref(&self) -> Backend<'_, &T> {
        match self {
            Self::Console(c, x) => Backend::Console(MaybeOwned::Borrowed(c), x),
        }
    }
}

impl<'a, T> Backend<'a, T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`Backend<T>`] to [`Backend<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> Backend<'a, R> {
        match self {
            Self::Console(c, x) => Backend::Console(c, f(x)),
        }
    }
}

/// True to its name, this function encapsulates our current auth trade-offs.
/// Here, we choose the appropriate auth flow based on circumstances.
///
/// All authentication flows will emit an AuthenticationOk message if successful.
async fn auth_quirks(
    api: &impl console::Api,
    user_info: ComputeUserInfoMaybeEndpoint,
    client: &mut AuthProxyStream,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    let (info) = match user_info.try_into() {
        Err(info) => {
            todo!()
            // let res = hacks::password_hack_no_authentication(info, client).await?;

            // let password = match res.keys {
            //     ComputeCredentialKeys::Password(p) => p,
            //     ComputeCredentialKeys::AuthKeys(_) | ComputeCredentialKeys::None => {
            //         unreachable!("password hack should return a password")
            //     }
            // };
            // (res.info, Some(password))
        }
        Ok(info) => info,
    };

    dbg!("fetching user's authentication info");
    let cached_secret = api
        .get_role_secret(&RequestMonitoring::test(), &info)
        .await?;

    let (cached_entry, secret) = cached_secret.take_value();

    let secret = if let Some(secret) = secret {
        secret
    } else {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        dbg!("authentication info not found, mocking it");
        AuthSecret::Scram(scram::ServerSecret::mock(rand::random()))
    };

    match authenticate_with_secret(secret, info, client, config).await {
        Ok(keys) => Ok(keys),
        Err(e) => {
            if e.is_auth_failed() {
                // The password could have been changed, so we invalidate the cache.
                cached_entry.invalidate();
            }
            Err(e)
        }
    }
}

async fn authenticate_with_secret(
    secret: AuthSecret,
    info: ComputeUserInfo,
    client: &mut AuthProxyStream,
    // unauthenticated_password: Option<Vec<u8>>,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials> {
    // if let Some(password) = unauthenticated_password {
    //     let ep = EndpointIdInt::from(&info.endpoint);

    //     let auth_outcome =
    //         validate_password_and_exchange(&config.thread_pool, ep, &password, secret).await?;
    //     let keys = match auth_outcome {
    //         crate::sasl::Outcome::Success(key) => key,
    //         crate::sasl::Outcome::Failure(reason) => {
    //             info!("auth backend failed with an error: {reason}");
    //             return Err(auth::AuthError::auth_failed(&*info.user));
    //         }
    //     };

    //     // we have authenticated the password
    //     client.write_message_noflush(&pq_proto::BeMessage::AuthenticationOk)?;

    //     return Ok(ComputeCredentials { info, keys });
    // }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(info, client, config, secret).await
}

impl<'a> Backend<'a, ComputeUserInfoMaybeEndpoint> {
    /// Get username from the credentials.
    pub fn get_user(&self) -> &str {
        match self {
            Self::Console(_, user_info) => &user_info.user,
        }
    }

    pub async fn authenticate(
        self,
        client: &mut AuthProxyStream,
        config: &'static AuthenticationConfig,
    ) -> auth::Result<Backend<'a, ComputeCredentials>> {
        let res = match self {
            Self::Console(api, user_info) => {
                dbg!("authenticating...");
                info!(
                    user = &*user_info.user,
                    project = user_info.endpoint(),
                    "performing authentication using the console"
                );

                let credentials = auth_quirks(&*api, user_info, client, config).await?;
                Backend::Console(api, credentials)
            }
        };

        dbg!("user successfully authenticated");
        Ok(res)
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for Backend<'_, ComputeCredentials> {
    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
    ) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        match self {
            Self::Console(api, creds) => api.wake_compute(ctx, &creds.info).await,
        }
    }

    fn get_keys(&self) -> &ComputeCredentialKeys {
        match self {
            Self::Console(_, creds) => &creds.keys,
        }
    }
}
