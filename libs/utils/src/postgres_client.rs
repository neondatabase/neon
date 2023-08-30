//! Postgres client connection code common to other crates (safekeeper and
//! pageserver) which depends on tenant/timeline ids and thus not fitting into
//! postgres_connection crate.

use anyhow::Context;
use postgres_connection::{parse_host_port, PgConnectionConfig};

use crate::id::TenantTimelineId;

/// Create client config for fetching WAL from safekeeper on particular timeline.
/// listen_pg_addr_str is in form host:\[port\].
pub fn wal_stream_connection_config(
    TenantTimelineId {
        tenant_id,
        timeline_id,
    }: TenantTimelineId,
    listen_pg_addr_str: &str,
    auth_token: Option<&str>,
    availability_zone: Option<&str>,
) -> anyhow::Result<PgConnectionConfig> {
    let (host, port) =
        parse_host_port(listen_pg_addr_str).context("Unable to parse listen_pg_addr_str")?;
    let port = port.unwrap_or(5432);
    let mut connstr = PgConnectionConfig::new_host_port(host, port)
        .extend_options([
            "-c".to_owned(),
            format!("timeline_id={}", timeline_id),
            format!("tenant_id={}", tenant_id),
        ])
        .set_password(auth_token.map(|s| s.to_owned()));

    if let Some(availability_zone) = availability_zone {
        connstr = connstr.extend_options([format!("availability_zone={}", availability_zone)]);
    }

    Ok(connstr)
}
