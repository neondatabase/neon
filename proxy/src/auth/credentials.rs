//! User credentials used in authentication.

use crate::{
    compute,
    config::ProxyConfig,
    error::UserFacingError,
    stream::{PqStream, Stream},
};
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

    // project_name is passed as argument from options from url.
    // In case sni_data is missing: project_name is used to determine cluster name.
    // In case sni_data is available: project_name and sni_data should match (otherwise throws an error).
    pub project_name: Option<String>,
}

impl ClientCredentials {
    pub fn is_existing_user(&self) -> bool {
        // This logic will likely change in the future.
        self.user.ends_with("@zenith")
    }

    pub fn parse<T>(
        stream: &Stream<T>,
        mut options: HashMap<String, String>,
    ) -> Result<Self, ClientCredsParseError> {
        let mut get_param = |key| {
            options
                .remove(key)
                .ok_or(ClientCredsParseError::MissingKey(key))
        };

        let user = get_param("user")?;
        let dbname = get_param("database")?;
        let project_name = get_param("project").ok();

        Ok(Self {
            user,
            dbname,
            sni_data: stream.sni_hostname().map(|s| s.to_owned()),
            project_name,
        })
    }
}

#[derive(Debug, Error)]
pub enum ProjectNameError {
    #[error(
        "Project name is not specified. \
        EITHER please upgrade the postgres client library (libpq) for SNI support \
        OR pass the project name as a parameter: '&options=project%3D<project-name>'."
    )]
    Missing,

    #[error("SNI is malformed.")]
    Bad,

    #[error("Inconsistent project name inferred from SNI ({0}) and project option ({0}).")]
    Inconsistent(String, String),
}

impl UserFacingError for ProjectNameError {}

impl ClientCredentials {
    /// Determine project name from SNI or from connection string options.
    pub fn project_name(&self) -> Result<&str, ProjectNameError> {
        use ProjectNameError::*;

        let name_from_options = self.project_name.as_ref();
        let name_from_sni = self
            .sni_data
            .as_ref()
            .map(|n| Ok(n.split_once('.').ok_or(Bad)?.0))
            .transpose()?;

        match (name_from_sni, name_from_options) {
            (Some(a), Some(b)) if a != b => Err(Inconsistent(a.to_owned(), b.to_owned())),
            (Some(a), _) => Ok(a),
            (_, Some(b)) => Ok(b),
            _ => Err(Missing),
        }
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
