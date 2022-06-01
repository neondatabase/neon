//! User credentials used in authentication.

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

    // cluster_option is passed as argument from options from url.
    // To be used to determine cluster name in case sni_data is missing.
    pub project_option: Option<String>,
}

impl ClientCredentials {
    pub fn is_existing_user(&self) -> bool {
        // This logic will likely change in the future.
        self.user.ends_with("@zenith")
    }
}

#[derive(Debug, Error)]
pub enum ProjectNameError {
    #[error("SNI info is missing. EITHER please upgrade the postgres client library OR pass the project name as a parameter: '..&options=project:<project name>..'.")]
    Missing,

    #[error("SNI is malformed.")]
    Bad,
}

impl UserFacingError for ProjectNameError {}

impl ClientCredentials {
    /// Determine project name from SNI.
    pub fn project_name(&self) -> Result<&str, ProjectNameError> {
        let ret = match &self.sni_data {
            //if sni_data exists, use it to determine project name
            Some(sni_data) => {
                sni_data
                    .split_once('.')
                    .ok_or(ProjectNameError::Bad)?
                    .0
            }
            //otherwise use project_option if it was manually set thought ..&options=project:<name> parameter
            None => self
                .project_option
                .as_ref()
                .ok_or(ProjectNameError::Missing)?
                .as_str(),
        };
        Ok(ret)
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
        let dbname = get_param("database")?;
        let project = get_param("project");
        let project_option = match project {
            Ok(project) => Some(project),
            Err(_) => None,
        };

        Ok(Self {
            user,
            dbname,
            sni_data: None,
            project_option,
        })
    }
}

impl ClientCredentials {
    /// Use credentials to authenticate the user.
    pub async fn authenticate(
        self,
        config: &ProxyConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> super::Result<compute::NodeInfo> {
        // This method is just a convenient facade for `handle_user`
        super::backend::handle_user(config, client, self).await
    }
}
