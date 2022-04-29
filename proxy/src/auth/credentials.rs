//! User credentials used in authentication.

use super::AuthError;
use crate::compute::DatabaseInfo;
use crate::config::ProxyConfig;
use crate::error::UserFacingError;
use crate::stream::PqStream;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Error)]
pub enum ClientCredsParseError {
    #[error("Parameter `{0}` is missing in startup packet")]
    MissingKey(&'static str),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,
}

impl TryFrom<HashMap<String, String>> for ClientCredentials {
    type Error = ClientCredsParseError;

    fn try_from(mut value: HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut get_param = |key| {
            value
                .remove(key)
                .ok_or(ClientCredsParseError::MissingKey(key))
        };

        let user = get_param("user")?;
        let db = get_param("database")?;

        Ok(Self { user, dbname: db })
    }
}

impl ClientCredentials {
    /// Use credentials to authenticate the user.
    pub async fn authenticate(
        self,
        config: &ProxyConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    ) -> Result<DatabaseInfo, AuthError> {
        use crate::config::ClientAuthMethod::*;
        use crate::config::RouterConfig::*;
        match &config.router_config {
            Static { host, port } => super::handle_static(host.clone(), *port, client, self).await,
            Dynamic(Mixed) => {
                if self.user.ends_with("@zenith") {
                    super::handle_existing_user(config, client, self).await
                } else {
                    super::handle_new_user(config, client).await
                }
            }
            Dynamic(Password) => super::handle_existing_user(config, client, self).await,
            Dynamic(Link) => super::handle_new_user(config, client).await,
        }
    }
}
