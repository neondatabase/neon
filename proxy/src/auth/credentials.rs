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

    // project_name is passed as argument from options from url.
    // In case sni_data is missing: project_name is used to determine cluster name.
    // In case sni_data is available: project_name and sni_data should match (otherwise throws an error).
    pub project_name: Option<String>,

    // common name is extracted from server.crt and used as help to determine project name from SNI.
    pub common_name: Option<String>,
}

impl ClientCredentials {
    pub fn is_existing_user(&self) -> bool {
        // This logic will likely change in the future.
        self.user.ends_with("@zenith")
    }
}

#[derive(Debug, Error)]
pub enum ProjectNameError {
    #[error("SNI is missing. EITHER please upgrade the postgres client library OR pass the project name as a parameter: '...&options=project%3D<project-name>...'.")]
    Missing,

    #[error("Inconsistent project name inferred from SNI and project option. String from SNI: '{0}', String from project option: '{1}'")]
    InconsistentProjectNameAndSNI(String, String),

    #[error("Common name is not set.")]
    CommonNameNotSet,

    #[error("Inconsistent common name and SNI suffix. Expected common name: '{0}', SNI: '{1}'")]
    InconsistentCommonNameAndSNI(String, String),

    #[error("Project name must contain only alphanumeric characters and hyphens ('-'). Project name: '{0}'.")]
    ProjectNameContainsIllegalChars(String),
}

impl UserFacingError for ProjectNameError {}

/// Inferring project name from sni_data.
fn project_name_from_sni_data<'sni>(
    sni_data: &'sni str,
    common_name: &Option<String>,
) -> Result<&'sni str, ProjectNameError> {
    // extract common name. If unset, throw a CommonNameNotSet error.
    let common_name = match &common_name {
        Some(common_name) => common_name,
        None => return Err(ProjectNameError::CommonNameNotSet),
    };

    // check that the common name passed from common_name is the actual suffix in sni_data.
    use substring::Substring;
    let sni_suffix = sni_data.substring(sni_data.len() - common_name.len(), sni_data.len());
    if !sni_suffix.eq(common_name) {
        return Err(ProjectNameError::InconsistentCommonNameAndSNI(
            common_name.to_string(),
            sni_data.to_string(),
        ));
    }

    // return sni_data without the common name suffix.
    Ok(sni_data.substring(0, sni_data.len() - common_name.len()))
}

impl ClientCredentials {
    /// Determine project name from SNI or from project_name parameter from options argument.
    pub fn project_name(&self) -> Result<&str, ProjectNameError> {
        // Checking that if both sni_data and project_name are set, then they should match
        // otherwise, throws a ProjectNameError::Inconsistent error.
        if let Some(sni_data) = &self.sni_data {
            let project_name_from_sni_data =
                project_name_from_sni_data(sni_data, &self.common_name)?;
            if let Some(project_name_from_options) = &self.project_name {
                if !project_name_from_options.eq(project_name_from_sni_data) {
                    return Err(ProjectNameError::InconsistentProjectNameAndSNI(
                        project_name_from_sni_data.to_string(),
                        project_name_from_options.to_string(),
                    ));
                }
            }
        }

        // determine the project name from self.sni_data if it exists, otherwise from self.project_name.
        let ret = match &self.sni_data {
            // if sni_data exists, use it to determine project name
            Some(sni_data) => project_name_from_sni_data(sni_data, &self.common_name)?,
            // otherwise use project_option if it was manually set thought options parameter.
            None => self
                .project_name
                .as_ref()
                .ok_or(ProjectNameError::Missing)?
                .as_str(),
        };

        // checking that formatting invariant holds.
        // project name must contain only alphanumeric characters and hyphens.
        if !ret.chars().all(|x: char| x.is_alphanumeric() || x == '-') {
            return Err(ProjectNameError::ProjectNameContainsIllegalChars(
                ret.to_string(),
            ));
        }

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
        let project_name = get_param("project").ok();

        Ok(Self {
            user,
            dbname,
            sni_data: None,
            project_name,
            common_name: None,
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
