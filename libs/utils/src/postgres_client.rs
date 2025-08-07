//! Postgres client connection code common to other crates (safekeeper and
//! pageserver) which depends on tenant/timeline ids and thus not fitting into
//! postgres_connection crate.

use anyhow::Context;
use postgres_connection::{PgConnectionConfig, parse_host_port};

use crate::id::TenantTimelineId;
use crate::shard::ShardIdentity;

#[derive(Copy, Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InterpretedFormat {
    Bincode,
    Protobuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Compression {
    Zstd { level: i8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "args")]
#[serde(rename_all = "kebab-case")]
pub enum PostgresClientProtocol {
    /// Usual Postgres replication protocol
    Vanilla,
    /// Custom shard-aware protocol that replicates interpreted records.
    /// Used to send wal from safekeeper to pageserver.
    Interpreted {
        format: InterpretedFormat,
        compression: Option<Compression>,
    },
}

pub struct ConnectionConfigArgs<'a> {
    pub protocol: PostgresClientProtocol,

    pub ttid: TenantTimelineId,
    pub shard: Option<ShardIdentity>,

    pub listen_pg_addr_str: &'a str,

    pub auth_token: Option<&'a str>,
    pub availability_zone: Option<&'a str>,
}

impl<'a> ConnectionConfigArgs<'a> {
    fn options(&'a self) -> Vec<String> {
        // PostgreSQL connection parameters must be prefixed with "-c"
        let mut options = vec![
            "-c".to_owned(),
            format!("timeline_id={}", self.ttid.timeline_id),
            format!("tenant_id={}", self.ttid.tenant_id),
            // Serialize protocol config as JSON for PostgreSQL parameter parsing
            format!(
                "protocol={}",
                serde_json::to_string(&self.protocol).unwrap()
            ),
        ];

        if let Some(shard) = &self.shard {
            // Use .literal() for ShardCount to get u8 value, .0 for direct field access
            options.push(format!("shard_count={}", shard.count.literal()));
            options.push(format!("shard_number={}", shard.number.0));
            options.push(format!("shard_stripe_size={}", shard.stripe_size.0));
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
    // Use PostgreSQL standard port 5432 if not specified
    let port = port.unwrap_or(5432);
    let mut connstr = PgConnectionConfig::new_host_port(host, port)
        .extend_options(args.options())
        .set_password(args.auth_token.map(|s| s.to_owned()));

    if let Some(availability_zone) = args.availability_zone {
        connstr = connstr.extend_options([format!("availability_zone={availability_zone}")]);
    }

    Ok(connstr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{TenantId, TimelineId};
    use crate::shard::{ShardCount, ShardNumber, ShardStripeSize};

    #[test]
    fn test_connection_config_args_with_shard() {
        let ttid = TenantTimelineId::new(TenantId::generate(), TimelineId::generate());
        let shard = ShardIdentity::new(
            ShardNumber(0),
            ShardCount::new(4),
            ShardStripeSize(32768),
        )
        .unwrap();

        let args = ConnectionConfigArgs {
            protocol: PostgresClientProtocol::Vanilla,
            ttid,
            shard: Some(shard),
            listen_pg_addr_str: "localhost:5432",
            auth_token: None,
            availability_zone: None,
        };

        let options = args.options();

        // Verify basic options are present
        assert!(options.contains(&"-c".to_string()));
        assert!(options
            .iter()
            .any(|opt| opt.starts_with("timeline_id=")));
        assert!(options.iter().any(|opt| opt.starts_with("tenant_id=")));
        assert!(options.iter().any(|opt| opt.starts_with("protocol=")));

        // Verify shard parameters are included with correct values
        assert!(options
            .iter()
            .any(|opt| opt == "shard_count=4"), "shard_count should be 4");
        assert!(options
            .iter()
            .any(|opt| opt == "shard_number=0"), "shard_number should be 0");
        assert!(options
            .iter()
            .any(|opt| opt == "shard_stripe_size=32768"), "shard_stripe_size should be 32768");
    }

    #[test]
    fn test_connection_config_args_without_shard() {
        let ttid = TenantTimelineId::new(TenantId::generate(), TimelineId::generate());

        let args = ConnectionConfigArgs {
            protocol: PostgresClientProtocol::Vanilla,
            ttid,
            shard: None,
            listen_pg_addr_str: "localhost:5432",
            auth_token: None,
            availability_zone: None,
        };

        let options = args.options();

        // Verify basic options are present
        assert!(options.contains(&"-c".to_string()));
        assert!(options
            .iter()
            .any(|opt| opt.starts_with("timeline_id=")));
        assert!(options.iter().any(|opt| opt.starts_with("tenant_id=")));
        assert!(options.iter().any(|opt| opt.starts_with("protocol=")));

        // Verify no shard parameters are included
        assert!(
            !options.iter().any(|opt| opt.contains("shard_count")),
            "shard_count should not be present"
        );
        assert!(
            !options.iter().any(|opt| opt.contains("shard_number")),
            "shard_number should not be present"
        );
        assert!(
            !options.iter().any(|opt| opt.contains("shard_stripe_size")),
            "shard_stripe_size should not be present"
        );
    }

    #[test]
    fn test_connection_config_with_interpreted_protocol() {
        let ttid = TenantTimelineId::new(TenantId::generate(), TimelineId::generate());
        let shard = ShardIdentity::new(
            ShardNumber(2),
            ShardCount::new(8),
            ShardStripeSize(8192),
        )
        .unwrap();

        let args = ConnectionConfigArgs {
            protocol: PostgresClientProtocol::Interpreted {
                format: InterpretedFormat::Protobuf,
                compression: Some(Compression::Zstd { level: 3 }),
            },
            ttid,
            shard: Some(shard),
            listen_pg_addr_str: "10.0.0.1:5433",
            auth_token: Some("test_token"),
            availability_zone: Some("us-east-1"),
        };

        let options = args.options();

        // Verify protocol is serialized correctly
        let protocol_option = options
            .iter()
            .find(|opt| opt.starts_with("protocol="))
            .expect("protocol option should be present");
        assert!(protocol_option.contains("interpreted"));
        assert!(protocol_option.contains("protobuf"));

        // Verify shard parameters
        assert!(options
            .iter()
            .any(|opt| opt == "shard_count=8"));
        assert!(options
            .iter()
            .any(|opt| opt == "shard_number=2"));
        assert!(options
            .iter()
            .any(|opt| opt == "shard_stripe_size=8192"));
    }

    #[test]
    fn test_wal_stream_connection_config_with_shard() {
        let ttid = TenantTimelineId::new(TenantId::generate(), TimelineId::generate());
        let shard = ShardIdentity::new(
            ShardNumber(1),
            ShardCount::new(4),
            ShardStripeSize(32768),
        )
        .unwrap();

        let args = ConnectionConfigArgs {
            protocol: PostgresClientProtocol::Vanilla,
            ttid,
            shard: Some(shard),
            listen_pg_addr_str: "192.168.1.1:5432",
            auth_token: Some("secret_token"),
            availability_zone: Some("us-west-2"),
        };

        let config = wal_stream_connection_config(args).unwrap();

        // Verify port (host comparison is complex due to url::Host type)
        assert_eq!(config.port(), 5432);

        // Verify address format includes correct host
        assert_eq!(config.raw_address(), "192.168.1.1:5432");
    }

    #[test]
    fn test_wal_stream_connection_config_without_shard() {
        let ttid = TenantTimelineId::new(TenantId::generate(), TimelineId::generate());

        let args = ConnectionConfigArgs {
            protocol: PostgresClientProtocol::Vanilla,
            ttid,
            shard: None,
            listen_pg_addr_str: "example.com",
            auth_token: None,
            availability_zone: None,
        };

        let config = wal_stream_connection_config(args).unwrap();

        // Verify default port
        assert_eq!(config.port(), 5432);
        
        // Verify address format
        assert_eq!(config.raw_address(), "example.com:5432");
    }

    #[test]
    fn test_options_format_consistency() {
        // This test ensures that the options format remains consistent
        // and matches what PostgreSQL connection expects
        let ttid = TenantTimelineId::new(TenantId::generate(), TimelineId::generate());
        let shard = ShardIdentity::new(
            ShardNumber(3),
            ShardCount::new(16),
            ShardStripeSize(16384),
        )
        .unwrap();

        let args = ConnectionConfigArgs {
            protocol: PostgresClientProtocol::Vanilla,
            ttid,
            shard: Some(shard),
            listen_pg_addr_str: "localhost:5432",
            auth_token: None,
            availability_zone: None,
        };

        let options = args.options();

        // First option should always be "-c"
        assert_eq!(options[0], "-c");

        // All other options should be key=value format
        for opt in &options[1..] {
            assert!(
                opt.contains('='),
                "Option '{}' should be in key=value format",
                opt
            );
        }

        // Verify specific shard option formats
        let shard_count_opt = options
            .iter()
            .find(|opt| opt.starts_with("shard_count="))
            .expect("shard_count option should exist");
        assert_eq!(shard_count_opt, "shard_count=16");

        let shard_number_opt = options
            .iter()
            .find(|opt| opt.starts_with("shard_number="))
            .expect("shard_number option should exist");
        assert_eq!(shard_number_opt, "shard_number=3");

        let shard_stripe_opt = options
            .iter()
            .find(|opt| opt.starts_with("shard_stripe_size="))
            .expect("shard_stripe_size option should exist");
        assert_eq!(shard_stripe_opt, "shard_stripe_size=16384");
    }
}
