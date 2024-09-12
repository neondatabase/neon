use super::{
    ComputeCredentialKeys, ComputeCredentials, ComputeUserInfo, ComputeUserInfoNoEndpoint,
};
use crate::{
    auth,
    auth_proxy::{self, AuthFlow, AuthProxyStream},
};
use tracing::{info, warn};

/// Workaround for clients which don't provide an endpoint (project) name.
/// Similar to [`authenticate_cleartext`], but there's a specific password format,
/// and passwords are not yet validated (we don't know how to validate them!)
pub(crate) async fn password_hack_no_authentication(
    info: ComputeUserInfoNoEndpoint,
    client: &mut AuthProxyStream,
) -> auth::Result<ComputeCredentials> {
    warn!("project not specified, resorting to the password hack auth flow");

    let payload = AuthFlow::new(client)
        .begin(auth_proxy::PasswordHack)
        .await?
        .get_password()
        .await?;

    info!(project = &*payload.endpoint, "received missing parameter");

    // Report tentative success; compute node will check the password anyway.
    Ok(ComputeCredentials {
        info: ComputeUserInfo {
            user: info.user,
            options: info.options,
            endpoint: payload.endpoint,
        },
        keys: ComputeCredentialKeys::Password(payload.password),
    })
}
