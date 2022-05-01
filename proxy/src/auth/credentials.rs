//! User credentials used in authentication.

use super::AuthError;
use crate::compute;
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
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,

    // New console API requires SNI info to determine the cluster name.
    // Other Auth backends don't need it.
    pub sni_data: Option<String>,
}

impl ClientCredentials {
    pub fn is_existing_user(&self) -> bool {
        // This logic will likely change in the future.
        self.user.ends_with("@zenith")
    }
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

        Ok(Self {
            user,
            dbname: db,
            sni_data: None,
        })
    }
}

impl ClientCredentials {
    /// Use credentials to authenticate the user.
    pub async fn authenticate(
        self,
        config: &ProxyConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> Result<compute::NodeInfo, AuthError> {
        // This method is just a convenient facade for `handle_user`
        super::handle_user(config, client, self).await
    }
}
