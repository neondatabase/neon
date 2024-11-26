//! Postgres client connection code common to other crates (safekeeper and
//! pageserver) which depends on tenant/timeline ids and thus not fitting into
//! postgres_connection crate.

use anyhow::Context;
use postgres_connection::{parse_host_port, PgConnectionConfig};

use crate::id::TenantTimelineId;

/// Postgres client protocol types
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
    Debug,
)]
#[strum(serialize_all = "kebab-case")]
#[repr(u8)]
pub enum PostgresClientProtocol {
    /// Usual Postgres replication protocol
    Vanilla,
    /// Custom shard-aware protocol that replicates interpreted records.
    /// Used to send wal from safekeeper to pageserver.
    Interpreted,
}

impl TryFrom<u8> for PostgresClientProtocol {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            v if v == (PostgresClientProtocol::Vanilla as u8) => PostgresClientProtocol::Vanilla,
            v if v == (PostgresClientProtocol::Interpreted as u8) => {
                PostgresClientProtocol::Interpreted
            }
            x => return Err(x),
        })
    }
}

pub struct ConnectionConfigArgs<'a> {
    pub protocol: PostgresClientProtocol,

    pub ttid: TenantTimelineId,
    pub shard_number: Option<u8>,
    pub shard_count: Option<u8>,
    pub shard_stripe_size: Option<u32>,

    pub listen_pg_addr_str: &'a str,

    pub auth_token: Option<&'a str>,
    pub availability_zone: Option<&'a str>,
}

impl<'a> ConnectionConfigArgs<'a> {
    fn options(&'a self) -> Vec<String> {
        let mut options = vec![
            "-c".to_owned(),
            format!("timeline_id={}", self.ttid.timeline_id),
            format!("tenant_id={}", self.ttid.tenant_id),
            format!("protocol={}", self.protocol as u8),
        ];

        if self.shard_number.is_some() {
            assert!(self.shard_count.is_some());
            assert!(self.shard_stripe_size.is_some());

            options.push(format!("shard_count={}", self.shard_count.unwrap()));
            options.push(format!("shard_number={}", self.shard_number.unwrap()));
            options.push(format!(
                "shard_stripe_size={}",
                self.shard_stripe_size.unwrap()
            ));
        }

        options
    }
}

/// Create client config for fetching WAL from safekeeper on particular timeline.
/// listen_pg_addr_str is in form host:\[port\].
pub fn wal_stream_connection_config(
    args: ConnectionConfigArgs,
) -> anyhow::Result<PgConnectionConfig> {
    let (host, port) =
        parse_host_port(args.listen_pg_addr_str).context("Unable to parse listen_pg_addr_str")?;
    let port = port.unwrap_or(5432);
    let mut connstr = PgConnectionConfig::new_host_port(host, port)
        .extend_options(args.options())
        .set_password(args.auth_token.map(|s| s.to_owned()));

    if let Some(availability_zone) = args.availability_zone {
        connstr = connstr.extend_options([format!("availability_zone={}", availability_zone)]);
    }

    Ok(connstr)
}
