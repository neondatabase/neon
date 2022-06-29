//! User credentials used in authentication.

use crate::compute;
use crate::config::ProxyConfig;
use crate::error::UserFacingError;
use crate::stream::PqStream;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ClientCredsParseError {
    #[error("Parameter `{0}` is missing in startup packet.")]
    MissingKey(&'static str),

    #[error(
        "Project name is not specified. \
        EITHER please upgrade the postgres client library (libpq) for SNI support \
        OR pass the project name as a parameter: '&options=project%3D<project-name>'."
    )]
    MissingSNIAndProjectName,

    #[error("Inconsistent project name inferred from SNI ('{0}') and project option ('{1}').")]
    InconsistentProjectNameAndSNI(String, String),

    #[error("Common name is not set.")]
    CommonNameNotSet,

    #[error(
        "SNI ('{1}') inconsistently formatted with respect to common name ('{0}'). \
        SNI should be formatted as '<project-name>.<common-name>'."
    )]
    InconsistentCommonNameAndSNI(String, String),

    #[error("Project name ('{0}') must contain only alphanumeric characters and hyphens ('-').")]
    ProjectNameContainsIllegalChars(String),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,
    pub project_name: Result<String, ClientCredsParseError>,
}

impl ClientCredentials {
    pub fn is_existing_user(&self) -> bool {
        // This logic will likely change in the future.
        self.user.ends_with("@zenith")
    }

    pub fn parse(
        mut options: HashMap<String, String>,
        sni_data: Option<&str>,
        common_name: Option<&str>,
    ) -> Result<Self, ClientCredsParseError> {
        let mut get_param = |key| {
            options
                .remove(key)
                .ok_or(ClientCredsParseError::MissingKey(key))
        };

        let user = get_param("user")?;
        let dbname = get_param("database")?;
        let project_name = get_param("project").ok();
        let project_name = get_project_name(sni_data, common_name, project_name.as_deref());

        Ok(Self {
            user,
            dbname,
            project_name,
        })
    }

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

/// Inferring project name from sni_data.
fn project_name_from_sni_data(
    sni_data: &str,
    common_name: &str,
) -> Result<String, ClientCredsParseError> {
    let common_name_with_dot = format!(".{common_name}");
    // check that ".{common_name_with_dot}" is the actual suffix in sni_data
    if !sni_data.ends_with(&common_name_with_dot) {
        return Err(ClientCredsParseError::InconsistentCommonNameAndSNI(
            common_name.to_string(),
            sni_data.to_string(),
        ));
    }
    // return sni_data without the common name suffix.
    Ok(sni_data
        .strip_suffix(&common_name_with_dot)
        .unwrap()
        .to_string())
}

#[cfg(test)]
mod tests_for_project_name_from_sni_data {
    use super::*;

    #[test]
    fn passing() {
        let target_project_name = "my-project-123";
        let common_name = "localtest.me";
        let sni_data = format!("{target_project_name}.{common_name}");
        assert_eq!(
            project_name_from_sni_data(&sni_data, common_name),
            Ok(target_project_name.to_string())
        );
    }

    #[test]
    fn throws_inconsistent_common_name_and_sni_data() {
        let target_project_name = "my-project-123";
        let common_name = "localtest.me";
        let wrong_suffix = "wrongtest.me";
        assert_eq!(common_name.len(), wrong_suffix.len());
        let wrong_common_name = format!("wrong{wrong_suffix}");
        let sni_data = format!("{target_project_name}.{wrong_common_name}");
        assert_eq!(
            project_name_from_sni_data(&sni_data, common_name),
            Err(ClientCredsParseError::InconsistentCommonNameAndSNI(
                common_name.to_string(),
                sni_data
            ))
        );
    }
}

/// Determine project name from SNI or from project_name parameter from options argument.
fn get_project_name(
    sni_data: Option<&str>,
    common_name: Option<&str>,
    project_name: Option<&str>,
) -> Result<String, ClientCredsParseError> {
    // determine the project name from sni_data if it exists, otherwise from project_name.
    let ret = match sni_data {
        Some(sni_data) => {
            let common_name = common_name.ok_or(ClientCredsParseError::CommonNameNotSet)?;
            let project_name_from_sni = project_name_from_sni_data(sni_data, common_name)?;
            // check invariant: project name from options and from sni should match
            if let Some(project_name) = &project_name {
                if !project_name_from_sni.eq(project_name) {
                    return Err(ClientCredsParseError::InconsistentProjectNameAndSNI(
                        project_name_from_sni,
                        project_name.to_string(),
                    ));
                }
            }
            project_name_from_sni
        }
        None => project_name
            .ok_or(ClientCredsParseError::MissingSNIAndProjectName)?
            .to_string(),
    };

    // check formatting invariant: project name must contain only alphanumeric characters and hyphens.
    if !ret.chars().all(|x: char| x.is_alphanumeric() || x == '-') {
        return Err(ClientCredsParseError::ProjectNameContainsIllegalChars(ret));
    }

    Ok(ret)
}

#[cfg(test)]
mod tests_for_project_name_only {
    use super::*;

    #[test]
    fn passing_from_sni_data_only() {
        let target_project_name = "my-project-123";
        let common_name = "localtest.me";
        let sni_data = format!("{target_project_name}.{common_name}");
        assert_eq!(
            get_project_name(Some(&sni_data), Some(common_name), None),
            Ok(target_project_name.to_string())
        );
    }

    #[test]
    fn throws_project_name_contains_illegal_chars_from_sni_data_only() {
        let project_name_prefix = "my-project";
        let project_name_suffix = "123";
        let common_name = "localtest.me";

        for illegal_char_id in 0..256 {
            let illegal_char = char::from_u32(illegal_char_id).unwrap();
            if !(illegal_char.is_alphanumeric() || illegal_char == '-')
                && illegal_char.to_string().len() == 1
            {
                let target_project_name =
                    format!("{project_name_prefix}{illegal_char}{project_name_suffix}");
                let sni_data = format!("{target_project_name}.{common_name}");
                assert_eq!(
                    get_project_name(Some(&sni_data), Some(common_name), None),
                    Err(ClientCredsParseError::ProjectNameContainsIllegalChars(
                        target_project_name
                    ))
                );
            }
        }
    }

    #[test]
    fn passing_from_project_name_only() {
        let target_project_name = "my-project-123";
        let common_names = [Some("localtest.me"), None];
        for common_name in common_names {
            assert_eq!(
                get_project_name(None, common_name, Some(target_project_name)),
                Ok(target_project_name.to_string())
            );
        }
    }

    #[test]
    fn throws_project_name_contains_illegal_chars_from_project_name_only() {
        let project_name_prefix = "my-project";
        let project_name_suffix = "123";
        let common_names = [Some("localtest.me"), None];

        for common_name in common_names {
            for illegal_char_id in 0..256 {
                let illegal_char: char = char::from_u32(illegal_char_id).unwrap();
                if !(illegal_char.is_alphanumeric() || illegal_char == '-')
                    && illegal_char.to_string().len() == 1
                {
                    let target_project_name =
                        format!("{project_name_prefix}{illegal_char}{project_name_suffix}");
                    assert_eq!(
                        get_project_name(None, common_name, Some(&target_project_name)),
                        Err(ClientCredsParseError::ProjectNameContainsIllegalChars(
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
        let common_name = "localtest.me";
        let sni_data = format!("{target_project_name}.{common_name}");
        assert_eq!(
            get_project_name(
                Some(&sni_data),
                Some(common_name),
                Some(target_project_name)
            ),
            Ok(target_project_name.to_string())
        );
    }

    #[test]
    fn throws_inconsistent_project_name_and_sni() {
        let project_name_param = "my-project-123";
        let wrong_project_name = "not-my-project-123";
        let common_name = "localtest.me";
        let sni_data = format!("{wrong_project_name}.{common_name}");
        assert_eq!(
            get_project_name(Some(&sni_data), Some(common_name), Some(project_name_param)),
            Err(ClientCredsParseError::InconsistentProjectNameAndSNI(
                wrong_project_name.to_string(),
                project_name_param.to_string()
            ))
        );
    }

    #[test]
    fn throws_common_name_not_set() {
        let target_project_name = "my-project-123";
        let wrong_project_name = "not-my-project-123";
        let common_name = "localtest.me";
        let sni_datas = [
            Some(format!("{wrong_project_name}.{common_name}")),
            Some(format!("{target_project_name}.{common_name}")),
        ];
        let project_names = [None, Some(target_project_name)];
        for sni_data in sni_datas {
            for project_name_param in project_names {
                assert_eq!(
                    get_project_name(sni_data.as_deref(), None, project_name_param),
                    Err(ClientCredsParseError::CommonNameNotSet)
                );
            }
        }
    }

    #[test]
    fn throws_inconsistent_common_name_and_sni_data() {
        let target_project_name = "my-project-123";
        let wrong_project_name = "not-my-project-123";
        let common_name = "localtest.me";
        let wrong_suffix = "wrongtest.me";
        assert_eq!(common_name.len(), wrong_suffix.len());
        let wrong_common_name = format!("wrong{wrong_suffix}");
        let sni_datas = [
            Some(format!("{wrong_project_name}.{wrong_common_name}")),
            Some(format!("{target_project_name}.{wrong_common_name}")),
        ];
        let project_names = [None, Some(target_project_name)];
        for project_name_param in project_names {
            for sni_data in &sni_datas {
                assert_eq!(
                    get_project_name(sni_data.as_deref(), Some(common_name), project_name_param),
                    Err(ClientCredsParseError::InconsistentCommonNameAndSNI(
                        common_name.to_string(),
                        sni_data.clone().unwrap().to_string()
                    ))
                );
            }
        }
    }
}
