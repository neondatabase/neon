//! User credentials used in authentication.

use crate::{auth::password_hack::parse_endpoint_param, error::UserFacingError};
use itertools::Itertools;
use pq_proto::StartupMessageParams;
use std::collections::HashSet;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ClientCredsParseError {
    #[error("Parameter '{0}' is missing in startup packet.")]
    MissingKey(&'static str),

    #[error(
        "Inconsistent project name inferred from \
         SNI ('{}') and project option ('{}').",
        .domain, .option,
    )]
    InconsistentProjectNames { domain: String, option: String },

    #[error(
        "Common name inferred from SNI ('{}') is not known",
        .cn,
    )]
    UnknownCommonName { cn: String },

    #[error("Project name ('{0}') must contain only alphanumeric characters and hyphen.")]
    MalformedProjectName(String),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials<'a> {
    pub user: &'a str,
    // TODO: this is a severe misnomer! We should think of a new name ASAP.
    pub project: Option<String>,
}

impl ClientCredentials<'_> {
    #[inline]
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }
}

impl<'a> ClientCredentials<'a> {
    #[cfg(test)]
    pub fn new_noop() -> Self {
        ClientCredentials {
            user: "",
            project: None,
        }
    }

    pub fn parse(
        params: &'a StartupMessageParams,
        sni: Option<&str>,
        common_names: Option<HashSet<String>>,
    ) -> Result<Self, ClientCredsParseError> {
        use ClientCredsParseError::*;

        // Some parameters are stored in the startup message.
        let get_param = |key| params.get(key).ok_or(MissingKey(key));
        let user = get_param("user")?;

        // Project name might be passed via PG's command-line options.
        let project_option = params
            .options_raw()
            .and_then(|options| {
                // We support both `project` (deprecated) and `endpoint` options for backward compatibility.
                // However, if both are present, we don't exactly know which one to use.
                // Therefore we require that only one of them is present.
                options
                    .filter_map(parse_endpoint_param)
                    .at_most_one()
                    .ok()?
            })
            .map(|name| name.to_string());

        let project_from_domain = if let Some(sni_str) = sni {
            if let Some(cn) = common_names {
                let common_name_from_sni = sni_str.split_once('.').map(|(_, domain)| domain);

                let project = common_name_from_sni
                    .and_then(|domain| {
                        if cn.contains(domain) {
                            subdomain_from_sni(sni_str, domain)
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| UnknownCommonName {
                        cn: common_name_from_sni.unwrap_or("").into(),
                    })?;

                Some(project)
            } else {
                None
            }
        } else {
            None
        };

        let project = match (project_option, project_from_domain) {
            // Invariant: if we have both project name variants, they should match.
            (Some(option), Some(domain)) if option != domain => {
                Some(Err(InconsistentProjectNames { domain, option }))
            }
            // Invariant: project name may not contain certain characters.
            (a, b) => a.or(b).map(|name| match project_name_valid(&name) {
                false => Err(MalformedProjectName(name)),
                true => Ok(name),
            }),
        }
        .transpose()?;

        info!(user, project = project.as_deref(), "credentials");

        Ok(Self { user, project })
    }
}

fn project_name_valid(name: &str) -> bool {
    name.chars().all(|c| c.is_alphanumeric() || c == '-')
}

fn subdomain_from_sni(sni: &str, common_name: &str) -> Option<String> {
    sni.strip_suffix(common_name)?
        .strip_suffix('.')
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ClientCredsParseError::*;

    #[test]
    fn parse_bare_minimum() -> anyhow::Result<()> {
        // According to postgresql, only `user` should be required.
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let creds = ClientCredentials::parse(&options, None, None)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project, None);

        Ok(())
    }

    #[test]
    fn parse_excessive() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("database", "world"), // should be ignored
            ("foo", "bar"),        // should be ignored
        ]);

        let creds = ClientCredentials::parse(&options, None, None)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project, None);

        Ok(())
    }

    #[test]
    fn parse_project_from_sni() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let sni = Some("foo.localhost");
        let common_names = Some(["localhost".into()].into());

        let creds = ClientCredentials::parse(&options, sni, common_names)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project.as_deref(), Some("foo"));

        Ok(())
    }

    #[test]
    fn parse_project_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 project=bar -c geqo=off"),
        ]);

        let creds = ClientCredentials::parse(&options, None, None)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project.as_deref(), Some("bar"));

        Ok(())
    }

    #[test]
    fn parse_endpoint_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 endpoint=bar -c geqo=off"),
        ]);

        let creds = ClientCredentials::parse(&options, None, None)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project.as_deref(), Some("bar"));

        Ok(())
    }

    #[test]
    fn parse_three_endpoints_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            (
                "options",
                "-ckey=1 endpoint=one endpoint=two endpoint=three -c geqo=off",
            ),
        ]);

        let creds = ClientCredentials::parse(&options, None, None)?;
        assert_eq!(creds.user, "john_doe");
        assert!(creds.project.is_none());

        Ok(())
    }

    #[test]
    fn parse_when_endpoint_and_project_are_in_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 endpoint=bar project=foo -c geqo=off"),
        ]);

        let creds = ClientCredentials::parse(&options, None, None)?;
        assert_eq!(creds.user, "john_doe");
        assert!(creds.project.is_none());

        Ok(())
    }

    #[test]
    fn parse_projects_identical() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe"), ("options", "project=baz")]);

        let sni = Some("baz.localhost");
        let common_names = Some(["localhost".into()].into());

        let creds = ClientCredentials::parse(&options, sni, common_names)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project.as_deref(), Some("baz"));

        Ok(())
    }

    #[test]
    fn parse_multi_common_names() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let common_names = Some(["a.com".into(), "b.com".into()].into());
        let sni = Some("p1.a.com");
        let creds = ClientCredentials::parse(&options, sni, common_names)?;
        assert_eq!(creds.project.as_deref(), Some("p1"));

        let common_names = Some(["a.com".into(), "b.com".into()].into());
        let sni = Some("p1.b.com");
        let creds = ClientCredentials::parse(&options, sni, common_names)?;
        assert_eq!(creds.project.as_deref(), Some("p1"));

        Ok(())
    }

    #[test]
    fn parse_projects_different() {
        let options =
            StartupMessageParams::new([("user", "john_doe"), ("options", "project=first")]);

        let sni = Some("second.localhost");
        let common_names = Some(["localhost".into()].into());

        let err = ClientCredentials::parse(&options, sni, common_names).expect_err("should fail");
        match err {
            InconsistentProjectNames { domain, option } => {
                assert_eq!(option, "first");
                assert_eq!(domain, "second");
            }
            _ => panic!("bad error: {err:?}"),
        }
    }

    #[test]
    fn parse_inconsistent_sni() {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let sni = Some("project.localhost");
        let common_names = Some(["example.com".into()].into());

        let err = ClientCredentials::parse(&options, sni, common_names).expect_err("should fail");
        match err {
            UnknownCommonName { cn } => {
                assert_eq!(cn, "localhost");
            }
            _ => panic!("bad error: {err:?}"),
        }
    }
}
