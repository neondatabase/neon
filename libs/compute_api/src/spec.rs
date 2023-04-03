//! `ComputeSpec` represents the contents of the spec.json file.
//!
//! The spec.json file is used to pass information to 'compute_ctl'. It contains
//! all the information needed to start up the right version of PostgreSQL,
//! and connect it to the storage nodes.
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::str::FromStr;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

/// String type alias representing Postgres identifier and
/// intended to be used for DB / role names.
pub type PgIdent = String;

/// Cluster spec or configuration represented as an optional number of
/// delta operations + final cluster state description.
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ComputeSpecV2 {
    pub format_version: u64,

    // For debugging purposes only
    pub project_id: Option<String>,
    pub endpoint_id: Option<String>,
    pub operation_uuid: Option<String>,

    /// W3C trace context of the launch operation, for OpenTelemetry tracing
    pub startup_tracing_context: Option<HashMap<String, String>>,

    // Information needed to connect to the storage layer.
    //
    // `tenant_id`, `timeline_id` and `pageserver_connstring` are always needed.
    //
    // If Lsn == None, this is a primary endpoint that continues writing WAL at
    // the end of the timeline. If 'lsn' is set, this is a read-only node
    // "anchored" at that LSN. 'safekeeper_connstrings' must be non-empty for a
    // primary.
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde_as(as = "DisplayFromStr")]
    pub timeline_id: TimelineId,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub lsn: Option<Lsn>,
    pub pageserver_connstring: String,
    pub safekeeper_connstrings: Vec<String>,

    /// If set, 'storage_auth_token' is used as the password to authenticate to
    /// the pageserver and safekeepers.
    pub storage_auth_token: Option<String>,

    /// Contents of postgresql.conf file
    pub postgresql_conf: Option<String>,

    /// Extra settings to append to the postgresql.conf
    pub settings: GenericOptions,

    // Expected cluster state at the end of transition process.
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,
    pub extensions: Vec<PgIdent>,
    pub delta_operations: Option<Vec<DeltaOp>>,
}

#[derive(Deserialize)]
struct FormatVersionOnly {
    format_version: u64,
}

impl TryFrom<ComputeSpecAnyVersion> for ComputeSpecV2 {
    type Error = anyhow::Error;

    fn try_from(input: ComputeSpecAnyVersion) -> Result<ComputeSpecV2, anyhow::Error> {
        // First check the 'format_version' field
        match serde_json::from_value::<FormatVersionOnly>(input.0.clone())?.format_version {
            1 => {
                let v1: ComputeSpecV1 = serde_json::from_value(input.0)?;

                ComputeSpecV2::upgrade_from_v1(v1)
            }
            2 => {
                let v2: ComputeSpecV2 = serde_json::from_value(input.0)?;
                Ok(v2)
            }
            other => Err(anyhow::anyhow!(
                "unexpected format version {other} in spec file"
            )),
        }
    }
}

impl ComputeSpecV2 {
    pub fn parse_and_upgrade(input: &str) -> anyhow::Result<ComputeSpecV2> {
        ComputeSpecV2::try_from(ComputeSpecAnyVersion(serde_json::from_str::<
            serde_json::Value,
        >(input)?))
    }

    pub fn upgrade_from_v1(spec_v1: ComputeSpecV1) -> anyhow::Result<ComputeSpecV2> {
        let mut tenant_id = None;
        let mut timeline_id = None;
        let mut pageserver_connstring = None;
        let mut safekeeper_connstrings: Vec<String> = Vec::new();

        let mut extensions: Vec<String> = Vec::new();

        let mut settings: Vec<GenericOption> = Vec::new();
        for setting in &spec_v1.cluster.settings {
            if let Some(value) = &setting.value {
                match setting.name.as_str() {
                    "neon.tenant_id" => {
                        tenant_id = Some(TenantId::from_str(value)?);
                    }
                    "neon.timeline_id" => {
                        timeline_id = Some(TimelineId::from_str(value)?);
                    }
                    "neon.pageserver_connstring" => {
                        pageserver_connstring = Some(value.clone());
                    }
                    "neon.safekeepers" => {
                        // neon.safekeepers is a comma-separated list of poestgres connection URLs
                        safekeeper_connstrings =
                            value.split(',').map(|s| s.trim().to_string()).collect();
                    }
                    "shared_preload_libraries" => {
                        if value.contains("pg_stat_statements") {
                            extensions.push("pg_stat_statements".to_string());
                        }
                        settings.push(setting.clone())
                    }
                    _ => settings.push(setting.clone()),
                }
            } else {
                settings.push(setting.clone())
            }
        }
        let tenant_id =
            tenant_id.ok_or_else(|| anyhow!("neon.tenant_id missing from spec file"))?;
        let timeline_id =
            timeline_id.ok_or_else(|| anyhow!("neon.timeline_id missing from spec file"))?;
        let pageserver_connstring = pageserver_connstring
            .ok_or_else(|| anyhow!("neon.pageserver_connstring missing from spec file"))?;

        Ok(ComputeSpecV2 {
            format_version: 2,

            project_id: Some(spec_v1.cluster.cluster_id),
            endpoint_id: Some(spec_v1.cluster.name),
            operation_uuid: spec_v1.operation_uuid,

            startup_tracing_context: spec_v1.startup_tracing_context,

            tenant_id,
            timeline_id,
            lsn: None, // Not supported in V1
            pageserver_connstring,
            safekeeper_connstrings,

            storage_auth_token: spec_v1.storage_auth_token,

            postgresql_conf: None,
            settings: Some(settings),

            roles: spec_v1.cluster.roles,
            databases: spec_v1.cluster.databases,
            extensions,
            delta_operations: spec_v1.delta_operations,
        })
    }
}

#[serde_as]
#[derive(Deserialize, Debug)]
pub struct ComputeSpecAnyVersion(pub serde_json::Value);

// Old format that didn't have explicit 'tenant_id', 'timeline_id, 'pageserver_connstring'
// and 'safekeeper_connstrings' fields. They were stored in as GUCS in the 'cluster.settings'
// list
#[serde_as]
#[derive(Clone, Deserialize, Serialize)]
pub struct ComputeSpecV1 {
    pub format_version: u64,

    // The control plane also includes a 'timestamp' field in the JSON document,
    // but we don't use it for anything. Serde will ignore missing fields when
    // deserializing it.
    pub operation_uuid: Option<String>,
    pub cluster: ClusterV1,
    pub delta_operations: Option<Vec<DeltaOp>>,
    pub storage_auth_token: Option<String>,

    pub startup_tracing_context: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClusterV1 {
    pub cluster_id: String,
    pub name: String,
    pub state: Option<String>,
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,
    pub settings: Vec<GenericOption>,
}

/// Single cluster state changing operation that could not be represented as
/// a static `Cluster` structure. For example:
/// - DROP DATABASE
/// - DROP ROLE
/// - ALTER ROLE name RENAME TO new_name
/// - ALTER DATABASE name RENAME TO new_name
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeltaOp {
    pub action: String,
    pub name: PgIdent,
    pub new_name: Option<PgIdent>,
}

/// Rust representation of Postgres role info with only those fields
/// that matter for us.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Role {
    pub name: PgIdent,
    pub encrypted_password: Option<String>,
    pub options: GenericOptions,
}

/// Rust representation of Postgres database info with only those fields
/// that matter for us.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Database {
    pub name: PgIdent,
    pub owner: PgIdent,
    pub options: GenericOptions,
}

/// Common type representing both SQL statement params with or without value,
/// like `LOGIN` or `OWNER username` in the `CREATE/ALTER ROLE`, and config
/// options like `wal_level = logical`.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct GenericOption {
    pub name: String,
    pub value: Option<String>,
    pub vartype: String,
}

/// Optional collection of `GenericOption`'s. Type alias allows us to
/// declare a `trait` on it.
pub type GenericOptions = Option<Vec<GenericOption>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upgrade_v1_to_v2() -> anyhow::Result<()> {
        let spec_v1_str = std::fs::read_to_string("tests/spec-v1.json").unwrap();
        let spec_v2 = ComputeSpecV2::parse_and_upgrade(&spec_v1_str)?;

        // The original V1 file contains also neon.tenant_id, neon.timeline_id,
        // neon.pageserver_connstring and neon.safekeepers. They are put to exclicit
        // fields at the top level in V2.
        assert_eq!(
            spec_v2.tenant_id,
            TenantId::from_str("3d1f7595b468230304e0b73cecbcb081")?
        );
        assert_eq!(
            spec_v2.timeline_id,
            TimelineId::from_str("7f2aff2a1042b93a2617f44851638422")?
        );
        assert_eq!(spec_v2.pageserver_connstring, "host=172.30.42.12 port=6400");
        assert_eq!(
            spec_v2.safekeeper_connstrings,
            vec![
                "172.30.42.23:6500",
                "172.30.42.22:6500",
                "172.30.42.21:6500"
            ]
        );

        fn opt(name: &str, value: &str, vartype: &str) -> GenericOption {
            GenericOption {
                name: name.to_string(),
                value: Some(value.to_string()),
                vartype: vartype.to_string(),
            }
        }

        assert_eq!(spec_v2.postgresql_conf, None);
        assert_eq!(
            spec_v2.settings.as_ref().unwrap(),
            &vec![
                opt("max_replication_write_lag", "500", "integer"),
                opt("restart_after_crash", "off", "bool"),
                opt("password_encryption", "md5", "enum"),
                opt(
                    "shared_preload_libraries",
                    "neon, pg_stat_statements",
                    "string"
                ),
                opt("synchronous_standby_names", "walproposer", "string"),
                opt("wal_level", "replica", "enum"),
                opt("listen_addresses", "0.0.0.0", "string"),
                opt("neon.max_cluster_size", "10240", "integer"),
                opt("shared_buffers", "65536", "integer"),
                opt(
                    "test.escaping",
                    r#"here's a backslash \ and a quote ' and a double-quote " hooray"#,
                    "string"
                ),
            ]
        );

        assert_eq!(spec_v2.extensions, vec!["pg_stat_statements"]);

        eprintln!("SPEC: {}", serde_json::to_string_pretty(&spec_v2)?);

        Ok(())
    }
}
