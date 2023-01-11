//! User credentials used in authentication.

use crate::error::UserFacingError;
use pq_proto::StartupMessageParams;
use std::borrow::Cow;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ClientCredsParseError {
    #[error("Parameter '{0}' is missing in startup packet.")]
    MissingKey(&'static str),

    #[error("Inconsistent project name inferred from SNI ('{}') and project option ('{}').", .domain, .option)]
    InconsistentProjectNames { domain: String, option: String },

    #[error(
        "SNI ('{}') inconsistently formatted with respect to common name ('{}'). \
        SNI should be formatted as '<project-name>.{}'.",
        .sni, .cn, .cn,
    )]
    InconsistentSni { sni: String, cn: String },

    #[error("Project name ('{0}') must contain only alphanumeric characters and hyphen.")]
    MalformedProjectName(String),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials<'a> {
    pub user: &'a str,
    pub dbname: &'a str,
    // TODO: this is a severe misnomer! We should think of a new name ASAP.
    pub project: Option<Cow<'a, str>>,
    /// If `True`, we'll use the old cleartext password flow. This is used for
    /// websocket connections, which want to minimize the number of round trips.
    pub use_cleartext_password_flow: bool,
}

impl ClientCredentials<'_> {
    #[inline]
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }
}

impl<'a> ClientCredentials<'a> {
    #[inline]
    pub fn as_ref(&'a self) -> ClientCredentials<'a> {
        Self {
            user: self.user,
            dbname: self.dbname,
            project: self.project().map(Cow::Borrowed),
            use_cleartext_password_flow: self.use_cleartext_password_flow,
        }
    }
}

impl<'a> ClientCredentials<'a> {
    pub fn parse(
        params: &'a StartupMessageParams,
        sni: Option<&str>,
        common_name: Option<&str>,
        use_cleartext_password_flow: bool,
    ) -> Result<Self, ClientCredsParseError> {
        use ClientCredsParseError::*;

        // Some parameters are stored in the startup message.
        let get_param = |key| params.get(key).ok_or(MissingKey(key));
        let user = get_param("user")?;
        let dbname = get_param("database")?;

        // Project name might be passed via PG's command-line options.
        let project_option = params.options_raw().and_then(|mut options| {
            options
                .find_map(|opt| opt.strip_prefix("project="))
                .map(Cow::Borrowed)
        });

        // Alternative project name is in fact a subdomain from SNI.
        // NOTE: we do not consider SNI if `common_name` is missing.
        let project_domain = sni
            .zip(common_name)
            .map(|(sni, cn)| {
                subdomain_from_sni(sni, cn)
                    .ok_or_else(|| InconsistentSni {
                        sni: sni.into(),
                        cn: cn.into(),
                    })
                    .map(Cow::<'static, str>::Owned)
            })
            .transpose()?;

        let project = match (project_option, project_domain) {
            // Invariant: if we have both project name variants, they should match.
            (Some(option), Some(domain)) if option != domain => {
                Some(Err(InconsistentProjectNames {
                    domain: domain.into(),
                    option: option.into(),
                }))
            }
            // Invariant: project name may not contain certain characters.
            (a, b) => a.or(b).map(|name| match project_name_valid(&name) {
                false => Err(MalformedProjectName(name.into())),
                true => Ok(name),
            }),
        }
        .transpose()?;

        info!(
            user = user,
            dbname = dbname,
            project = project.as_deref(),
            use_cleartext_password_flow = use_cleartext_password_flow,
            "credentials"
        );

        Ok(Self {
            user,
            dbname,
            project,
            use_cleartext_password_flow,
        })
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
    #[ignore = "TODO: fix how database is handled"]
    fn parse_bare_minimum() -> anyhow::Result<()> {
        // According to postgresql, only `user` should be required.
        let options = StartupMessageParams::new([("user", "john_doe")]);

        // TODO: check that `creds.dbname` is None.
        let creds = ClientCredentials::parse(&options, None, None, false)?;
        assert_eq!(creds.user, "john_doe");

        Ok(())
    }

    #[test]
    fn parse_missing_project() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe"), ("database", "world")]);

        let creds = ClientCredentials::parse(&options, None, None, false)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.dbname, "world");
        assert_eq!(creds.project, None);

        Ok(())
    }

    #[test]
    fn parse_project_from_sni() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe"), ("database", "world")]);

        let sni = Some("foo.localhost");
        let common_name = Some("localhost");

        let creds = ClientCredentials::parse(&options, sni, common_name, false)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.dbname, "world");
        assert_eq!(creds.project.as_deref(), Some("foo"));

        Ok(())
    }

    #[test]
    fn parse_project_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("database", "world"),
            ("options", "-ckey=1 project=bar -c geqo=off"),
        ]);

        let creds = ClientCredentials::parse(&options, None, None, false)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.dbname, "world");
        assert_eq!(creds.project.as_deref(), Some("bar"));

        Ok(())
    }

    #[test]
    fn parse_projects_identical() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("database", "world"),
            ("options", "project=baz"),
        ]);

        let sni = Some("baz.localhost");
        let common_name = Some("localhost");

        let creds = ClientCredentials::parse(&options, sni, common_name, false)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.dbname, "world");
        assert_eq!(creds.project.as_deref(), Some("baz"));

        Ok(())
    }

    #[test]
    fn parse_projects_different() {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("database", "world"),
            ("options", "project=first"),
        ]);

        let sni = Some("second.localhost");
        let common_name = Some("localhost");

        let err =
            ClientCredentials::parse(&options, sni, common_name, false).expect_err("should fail");
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
        let options = StartupMessageParams::new([("user", "john_doe"), ("database", "world")]);

        let sni = Some("project.localhost");
        let common_name = Some("example.com");

        let err =
            ClientCredentials::parse(&options, sni, common_name, false).expect_err("should fail");
        match err {
            InconsistentSni { sni, cn } => {
                assert_eq!(sni, "project.localhost");
                assert_eq!(cn, "example.com");
            }
            _ => panic!("bad error: {err:?}"),
        }
    }
}
