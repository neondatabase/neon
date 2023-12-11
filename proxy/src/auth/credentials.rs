//! User credentials used in authentication.

use crate::{
    auth::password_hack::parse_endpoint_param,
    error::UserFacingError,
    proxy::{neon_options_str, NUM_CONNECTION_ACCEPTED_BY_SNI},
};
use itertools::Itertools;
use pq_proto::StartupMessageParams;
use smol_str::SmolStr;
use std::{collections::HashSet, net::IpAddr};
use thiserror::Error;
use tracing::{info, warn};

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ClientCredsParseError {
    #[error("Parameter '{0}' is missing in startup packet.")]
    MissingKey(&'static str),

    #[error(
        "Inconsistent project name inferred from \
         SNI ('{}') and project option ('{}').",
        .domain, .option,
    )]
    InconsistentProjectNames { domain: SmolStr, option: SmolStr },

    #[error(
        "Common name inferred from SNI ('{}') is not known",
        .cn,
    )]
    UnknownCommonName { cn: String },

    #[error("Project name ('{0}') must contain only alphanumeric characters and hyphen.")]
    MalformedProjectName(SmolStr),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: SmolStr,
    // TODO: this is a severe misnomer! We should think of a new name ASAP.
    pub project: Option<SmolStr>,

    pub cache_key: SmolStr,
    pub peer_addr: IpAddr,
}

impl ClientCredentials {
    #[inline]
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }
}

impl ClientCredentials {
    pub fn parse(
        params: &StartupMessageParams,
        sni: Option<&str>,
        common_names: Option<HashSet<String>>,
        peer_addr: IpAddr,
    ) -> Result<Self, ClientCredsParseError> {
        use ClientCredsParseError::*;

        // Some parameters are stored in the startup message.
        let get_param = |key| params.get(key).ok_or(MissingKey(key));
        let user = get_param("user")?.into();

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
            .map(|name| name.into());

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

        info!(%user, project = project.as_deref(), "credentials");
        if sni.is_some() {
            info!("Connection with sni");
            NUM_CONNECTION_ACCEPTED_BY_SNI
                .with_label_values(&["sni"])
                .inc();
        } else if project.is_some() {
            NUM_CONNECTION_ACCEPTED_BY_SNI
                .with_label_values(&["no_sni"])
                .inc();
            info!("Connection without sni");
        } else {
            NUM_CONNECTION_ACCEPTED_BY_SNI
                .with_label_values(&["password_hack"])
                .inc();
            info!("Connection with password hack");
        }

        let cache_key = format!(
            "{}{}",
            project.as_deref().unwrap_or(""),
            neon_options_str(params)
        )
        .into();

        Ok(Self {
            user,
            project,
            cache_key,
            peer_addr,
        })
    }
}

pub fn check_peer_addr_is_in_list(peer_addr: &IpAddr, ip_list: &[IpPattern]) -> bool {
    if ip_list.is_empty() {
        return true;
    }
    for ip in ip_list {
        if check_ip(peer_addr, ip) {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IpPattern {
    Subnet(ipnet::IpNet),
    Range(IpAddr, IpAddr),
    Single(IpAddr),
}

impl<'de> serde::Deserialize<'de> for IpPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ip = <&'de str>::deserialize(deserializer)?;
        Ok(Self::from_str_lossy(ip))
    }
}

impl std::str::FromStr for IpPattern {
    type Err = anyhow::Error;

    fn from_str(pattern: &str) -> Result<Self, Self::Err> {
        if pattern.contains('/') {
            let subnet: ipnet::IpNet = pattern.parse()?;
            return Ok(IpPattern::Subnet(subnet));
        }
        if let Some((start, end)) = pattern.split_once('-') {
            let start: IpAddr = start.parse()?;
            let end: IpAddr = end.parse()?;
            return Ok(IpPattern::Range(start, end));
        }
        let addr: IpAddr = pattern.parse()?;
        Ok(IpPattern::Single(addr))
    }
}

impl IpPattern {
    pub fn from_str_lossy(pattern: &str) -> Self {
        match pattern.parse() {
            Ok(pattern) => pattern,
            Err(err) => {
                warn!("Cannot parse ip: {}; err: {}", pattern, err);
                // We expect that all ip addresses from control plane are correct.
                // However, if some of them are broken, we still can check the others.
                Self::Single([0, 0, 0, 0].into())
            }
        }
    }
}

fn check_ip(ip: &IpAddr, pattern: &IpPattern) -> bool {
    match pattern {
        IpPattern::Subnet(subnet) => subnet.contains(ip),
        IpPattern::Range(start, end) => start <= ip && ip <= end,
        IpPattern::Single(addr) => addr == ip,
    }
}

fn project_name_valid(name: &str) -> bool {
    name.chars().all(|c| c.is_alphanumeric() || c == '-')
}

fn subdomain_from_sni(sni: &str, common_name: &str) -> Option<SmolStr> {
    sni.strip_suffix(common_name)?
        .strip_suffix('.')
        .map(SmolStr::from)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use ClientCredsParseError::*;

    #[test]
    fn parse_bare_minimum() -> anyhow::Result<()> {
        // According to postgresql, only `user` should be required.
        let options = StartupMessageParams::new([("user", "john_doe")]);
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, None, None, peer_addr)?;
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
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, None, None, peer_addr)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project, None);

        Ok(())
    }

    #[test]
    fn parse_project_from_sni() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let sni = Some("foo.localhost");
        let common_names = Some(["localhost".into()].into());

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, sni, common_names, peer_addr)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project.as_deref(), Some("foo"));
        assert_eq!(creds.cache_key, "foo");

        Ok(())
    }

    #[test]
    fn parse_project_from_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "-ckey=1 project=bar -c geqo=off"),
        ]);

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, None, None, peer_addr)?;
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

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, None, None, peer_addr)?;
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

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, None, None, peer_addr)?;
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

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, None, None, peer_addr)?;
        assert_eq!(creds.user, "john_doe");
        assert!(creds.project.is_none());

        Ok(())
    }

    #[test]
    fn parse_projects_identical() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe"), ("options", "project=baz")]);

        let sni = Some("baz.localhost");
        let common_names = Some(["localhost".into()].into());

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, sni, common_names, peer_addr)?;
        assert_eq!(creds.user, "john_doe");
        assert_eq!(creds.project.as_deref(), Some("baz"));

        Ok(())
    }

    #[test]
    fn parse_multi_common_names() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([("user", "john_doe")]);

        let common_names = Some(["a.com".into(), "b.com".into()].into());
        let sni = Some("p1.a.com");
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, sni, common_names, peer_addr)?;
        assert_eq!(creds.project.as_deref(), Some("p1"));

        let common_names = Some(["a.com".into(), "b.com".into()].into());
        let sni = Some("p1.b.com");
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, sni, common_names, peer_addr)?;
        assert_eq!(creds.project.as_deref(), Some("p1"));

        Ok(())
    }

    #[test]
    fn parse_projects_different() {
        let options =
            StartupMessageParams::new([("user", "john_doe"), ("options", "project=first")]);

        let sni = Some("second.localhost");
        let common_names = Some(["localhost".into()].into());

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let err = ClientCredentials::parse(&options, sni, common_names, peer_addr)
            .expect_err("should fail");
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

        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let err = ClientCredentials::parse(&options, sni, common_names, peer_addr)
            .expect_err("should fail");
        match err {
            UnknownCommonName { cn } => {
                assert_eq!(cn, "localhost");
            }
            _ => panic!("bad error: {err:?}"),
        }
    }

    #[test]
    fn parse_neon_options() -> anyhow::Result<()> {
        let options = StartupMessageParams::new([
            ("user", "john_doe"),
            ("options", "neon_lsn:0/2 neon_endpoint_type:read_write"),
        ]);

        let sni = Some("project.localhost");
        let common_names = Some(["localhost".into()].into());
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let creds = ClientCredentials::parse(&options, sni, common_names, peer_addr)?;
        assert_eq!(creds.project.as_deref(), Some("project"));
        assert_eq!(creds.cache_key, "projectendpoint_type:read_write lsn:0/2");

        Ok(())
    }

    #[test]
    fn test_check_peer_addr_is_in_list() {
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        assert!(check_peer_addr_is_in_list(&peer_addr, &[]));
        assert!(check_peer_addr_is_in_list(
            &peer_addr,
            &[IpPattern::from_str_lossy("127.0.0.1")]
        ));
        assert!(!check_peer_addr_is_in_list(
            &peer_addr,
            &[IpPattern::from_str_lossy("8.8.8.8")]
        ));
        // If there is an incorrect address, it will be skipped.
        assert!(check_peer_addr_is_in_list(
            &peer_addr,
            &[
                IpPattern::from_str_lossy("88.8.8"),
                IpPattern::from_str_lossy("127.0.0.1")
            ]
        ));
    }
    #[test]
    fn test_parse_ip_v4() -> anyhow::Result<()> {
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        // Ok
        assert_eq!(
            IpPattern::from_str("127.0.0.1")?,
            IpPattern::Single(peer_addr)
        );
        assert_eq!(
            IpPattern::from_str("127.0.0.1/31")?,
            IpPattern::Subnet(ipnet::IpNet::new(peer_addr, 31)?)
        );
        assert_eq!(
            IpPattern::from_str("0.0.0.0-200.0.1.2")?,
            IpPattern::Range(IpAddr::from([0, 0, 0, 0]), IpAddr::from([200, 0, 1, 2]))
        );

        // Error
        assert!(IpPattern::from_str("300.0.1.2").is_err());
        assert!(IpPattern::from_str("30.1.2").is_err());
        assert!(IpPattern::from_str("127.0.0.1/33").is_err());
        assert!(IpPattern::from_str("127.0.0.1-127.0.3").is_err());
        assert!(IpPattern::from_str("1234.0.0.1-127.0.3.0").is_err());
        Ok(())
    }

    #[test]
    fn test_check_ipv4() -> anyhow::Result<()> {
        let peer_addr = IpAddr::from([127, 0, 0, 1]);
        let peer_addr_next = IpAddr::from([127, 0, 0, 2]);
        let peer_addr_prev = IpAddr::from([127, 0, 0, 0]);
        // Success
        assert!(check_ip(&peer_addr, &IpPattern::Single(peer_addr)));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Subnet(ipnet::IpNet::new(peer_addr_prev, 31)?)
        ));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Subnet(ipnet::IpNet::new(peer_addr_next, 30)?)
        ));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Range(IpAddr::from([0, 0, 0, 0]), IpAddr::from([200, 0, 1, 2]))
        ));
        assert!(check_ip(
            &peer_addr,
            &IpPattern::Range(peer_addr, peer_addr)
        ));

        // Not success
        assert!(!check_ip(&peer_addr, &IpPattern::Single(peer_addr_prev)));
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Subnet(ipnet::IpNet::new(peer_addr_next, 31)?)
        ));
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Range(IpAddr::from([0, 0, 0, 0]), peer_addr_prev)
        ));
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Range(peer_addr_next, IpAddr::from([128, 0, 0, 0]))
        ));
        // There is no check that for range start <= end. But it's fine as long as for all this cases the result is false.
        assert!(!check_ip(
            &peer_addr,
            &IpPattern::Range(peer_addr, peer_addr_prev)
        ));
        Ok(())
    }
}
