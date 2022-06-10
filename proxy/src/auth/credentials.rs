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

#[derive(Debug, Error, PartialEq)]
pub enum ProjectNameError {
    #[error("SNI is missing. EITHER please upgrade the postgres client library OR pass the project name as a parameter: '...&options=project%3D<project-name>...'.")]
    Missing,

    #[error("Inconsistent project name inferred from SNI and project option. Project name from SNI: '{0}', Project name from project option: '{1}'")]
    InconsistentProjectNameAndSNI(String, String),

    #[error("Common name is not set.")]
    CommonNameNotSet,

    #[error("Inconsistent common name and SNI suffix. Common name: '{0}', SNI: '{1}', SNI suffix: '{2}'")]
    InconsistentCommonNameAndSNI(String, String, String),

    #[error("Project name must contain only alphanumeric characters and hyphens ('-'). Project name: '{0}'.")]
    ProjectNameContainsIllegalChars(String),
}

impl UserFacingError for ProjectNameError {}

/// Inferring project name from sni_data.
/// If common_name is None, throws an CommonNameNotSet error,
/// otherwise checks if common_name is suffix to sni_data.
/// if common_name is not suffix to sni_data throws InconsistentCommonNameAndSNI error,
/// otherwise returns the prefix of sni_data without the common_name suffix.
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
            sni_suffix.to_string(),
        ));
    }

    // return sni_data without the common name suffix.
    Ok(sni_data.substring(0, sni_data.len() - common_name.len()))
}

#[cfg(test)]
mod tests_for_project_name_from_sni_data {
    use super::*;

    #[test]
    fn passing() {
        let target_project_name = "my-project-123";
        let common_name = String::from(".localtest.me");
        let sni_data = target_project_name.to_string() + common_name.as_str();
        assert_eq!(
            project_name_from_sni_data(sni_data.as_str(), &Some(common_name)).ok(),
            Some(target_project_name)
        );
    }

    #[test]
    fn throws_common_name_not_set() {
        let target_project_name = "my-project-123";
        let common_name = String::from(".localtest.me");
        let sni_data = target_project_name.to_string() + common_name.as_str();
        assert_eq!(
            project_name_from_sni_data(sni_data.as_str(), &None).err(),
            Some(ProjectNameError::CommonNameNotSet)
        );
    }

    #[test]
    fn throws_inconsistent_common_name_and_sni_data() {
        let target_project_name = "my-project-123";
        let common_name = String::from(".localtest.me");
        let wrong_suffix = "_localtest.me";
        assert_eq!(common_name.len(), wrong_suffix.len());
        let wrong_common_name = ".wrong".to_string() + wrong_suffix;
        let sni_data = target_project_name.to_string() + wrong_common_name.as_str();
        assert_eq!(
            project_name_from_sni_data(sni_data.as_str(), &Some(common_name.clone())).err(),
            Some(ProjectNameError::InconsistentCommonNameAndSNI(
                common_name,
                sni_data,
                wrong_suffix.to_string()
            ))
        );
    }
}

/// Determine project name from SNI or from project_name parameter from options argument.
fn project_name<'ret>(
    sni_data: &'ret Option<String>,
    common_name: &Option<String>,
    project_name: &'ret Option<String>,
) -> Result<&'ret str, ProjectNameError> {
    // Checking that if both sni_data and project_name are set, then they should match
    // otherwise, throws a ProjectNameError::Inconsistent error.
    if let Some(sni_data) = sni_data {
        let project_name_from_sni_data = project_name_from_sni_data(sni_data, common_name)?;
        if let Some(project_name_from_options) = &project_name {
            if !project_name_from_options.eq(project_name_from_sni_data) {
                return Err(ProjectNameError::InconsistentProjectNameAndSNI(
                    project_name_from_sni_data.to_string(),
                    project_name_from_options.to_string(),
                ));
            }
        }
    }

    // determine the project name from self.sni_data if it exists, otherwise from self.project_name.
    let ret = match &sni_data {
        // if sni_data exists, use it to determine project name
        Some(sni_data) => project_name_from_sni_data(sni_data, &common_name)?,
        // otherwise use project_option if it was manually set thought options parameter.
        None => project_name
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

#[cfg(test)]
mod tests_for_project_name_only {
    use super::*;

    #[test]
    fn passing_from_sni_data_only() {
        let target_project_name = "my-project-123";
        let common_name = String::from(".localtest.me");
        let sni_data = target_project_name.to_string() + common_name.as_str();
        assert_eq!(
            project_name(&Some(sni_data), &Some(common_name), &None).ok(),
            Some(target_project_name)
        );
    }

    #[test]
    fn throws_project_name_contains_illegal_chars_from_sni_data_only() {
        let project_name_prefix = "my-project";
        let project_name_suffix = "123";
        let common_name = String::from(".localtest.me");

        for illegal_char_id in 0..256 {
            let illegal_char = char::from_u32(illegal_char_id).unwrap();
            if !(illegal_char.is_alphanumeric() || illegal_char == '-')
                && illegal_char.to_string().len() == 1
            {
                let target_project_name = project_name_prefix.to_string()
                    + illegal_char.to_string().as_str()
                    + project_name_suffix;
                let sni_data = target_project_name.to_string() + common_name.as_str();
                assert_eq!(
                    project_name(&Some(sni_data), &Some(common_name.clone()), &None).err(),
                    Some(ProjectNameError::ProjectNameContainsIllegalChars(
                        target_project_name
                    ))
                );
            }
        }
    }

    #[test]
    fn passing_from_project_name_only() {
        let target_project_name = "my-project-123";
        let common_names = [Some(String::from(".localtest.me")), None];
        for common_name in common_names {
            assert_eq!(
                project_name(&None, &common_name, &Some(target_project_name.to_string())).ok(),
                Some(target_project_name)
            );
        }
    }

    #[test]
    fn throws_project_name_contains_illegal_chars_from_project_name_only() {
        let project_name_prefix = "my-project";
        let project_name_suffix = "123";
        let common_names = [Some(String::from(".localtest.me")), None];

        for common_name in common_names {
            for illegal_char_id in 0..256 {
                let illegal_char: char = char::from_u32(illegal_char_id).unwrap();
                if !(illegal_char.is_alphanumeric() || illegal_char == '-')
                    && illegal_char.to_string().len() == 1
                {
                    let target_project_name = project_name_prefix.to_string()
                        + illegal_char.to_string().as_str()
                        + project_name_suffix;
                    assert_eq!(
                        project_name(&None, &common_name, &Some(target_project_name.to_string()))
                            .err(),
                        Some(ProjectNameError::ProjectNameContainsIllegalChars(
                            target_project_name
                        ))
                    );
                }
            }
        }
    }

    #[test]
    fn passing_from_sni_data_and_project_name() {
        let target_project_name = "my-project-123";
        let common_name = String::from(".localtest.me");
        let sni_data = target_project_name.to_string() + common_name.as_str();
        assert_eq!(
            project_name(
                &Some(sni_data),
                &Some(common_name),
                &Some(target_project_name.to_string())
            )
            .ok(),
            Some(target_project_name)
        );
    }

    #[test]
    fn throws_inconsistent_project_name_and_sni() {
        let project_name_param = "my-project-123";
        let wrong_project_name = "not-my-project-123";
        let common_name = String::from(".localtest.me");
        let sni_data = wrong_project_name.to_string() + common_name.as_str();
        assert_eq!(
            project_name(
                &Some(sni_data.clone()),
                &Some(common_name),
                &Some(project_name_param.to_string())
            )
            .err(),
            Some(ProjectNameError::InconsistentProjectNameAndSNI(
                wrong_project_name.to_string(),
                project_name_param.to_string()
            ))
        );
    }

    #[test]
    fn throws_common_name_not_set() {
        let target_project_name = "my-project-123";
        let wrong_project_name = "not-my-project-123";
        let common_name = String::from(".localtest.me");
        let sni_datas = [
            Some(wrong_project_name.to_string() + common_name.as_str()),
            Some(target_project_name.to_string() + common_name.as_str()),
        ];
        let project_names = [None, Some(target_project_name.to_string())];
        for sni_data in sni_datas {
            for project_name_param in &project_names {
                assert_eq!(
                    project_name(&sni_data, &None, &project_name_param).err(),
                    Some(ProjectNameError::CommonNameNotSet)
                );
            }
        }
    }

    #[test]
    fn throws_inconsistent_common_name_and_sni_data() {
        let target_project_name = "my-project-123";
        let wrong_project_name = "not-my-project-123";
        let common_name = String::from(".localtest.me");
        let wrong_suffix = "_localtest.me";
        assert_eq!(common_name.len(), wrong_suffix.len());
        let wrong_common_name = ".wrong".to_string() + wrong_suffix;
        let sni_datas = [
            Some(wrong_project_name.to_string() + wrong_common_name.as_str()),
            Some(target_project_name.to_string() + wrong_common_name.as_str()),
        ];
        let project_names = [None, Some(target_project_name.to_string())];
        for sni_data in sni_datas {
            for project_name_param in &project_names {
                assert_eq!(
                    project_name(
                        &sni_data.clone(),
                        &Some(common_name.clone()),
                        &project_name_param
                    )
                    .err(),
                    Some(ProjectNameError::InconsistentCommonNameAndSNI(
                        common_name.to_string(),
                        sni_data.clone().unwrap(),
                        wrong_suffix.to_string()
                    ))
                );
            }
        }
    }
}

impl ClientCredentials {
    /// Determine project name from SNI or from project_name parameter from options argument.
    pub fn project_name(&self) -> Result<&str, ProjectNameError> {
        return project_name(&self.sni_data, &self.common_name, &self.project_name);
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
