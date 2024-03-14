use std::str::FromStr;

/// Request/response types for the storage controller
/// API (`/control/v1` prefix).  Implemented by the server
/// in [`attachment_service::http`]
use serde::{Deserialize, Serialize};
use utils::id::NodeId;

use crate::{models::ShardParameters, shard::TenantShardId};

#[derive(Serialize, Deserialize)]
pub struct TenantCreateResponseShard {
    pub shard_id: TenantShardId,
    pub node_id: NodeId,
    pub generation: u32,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateResponse {
    pub shards: Vec<TenantCreateResponseShard>,
}

#[derive(Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct NodeConfigureRequest {
    pub node_id: NodeId,

    pub availability: Option<NodeAvailability>,
    pub scheduling: Option<NodeSchedulingPolicy>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantLocateResponseShard {
    pub shard_id: TenantShardId,
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct TenantLocateResponse {
    pub shards: Vec<TenantLocateResponseShard>,
    pub shard_params: ShardParameters,
}

/// Explicitly migrating a particular shard is a low level operation
/// TODO: higher level "Reschedule tenant" operation where the request
/// specifies some constraints, e.g. asking it to get off particular node(s)
#[derive(Serialize, Deserialize, Debug)]
pub struct TenantShardMigrateRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub enum NodeAvailability {
    // Normal, happy state
    Active,
    // Offline: Tenants shouldn't try to attach here, but they may assume that their
    // secondary locations on this node still exist.  Newly added nodes are in this
    // state until we successfully contact them.
    Offline,
}

impl FromStr for NodeAvailability {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "offline" => Ok(Self::Offline),
            _ => Err(anyhow::anyhow!("Unknown availability state '{s}'")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub enum NodeSchedulingPolicy {
    Active,
    Filling,
    Pause,
    Draining,
}

impl FromStr for NodeSchedulingPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "filling" => Ok(Self::Filling),
            "pause" => Ok(Self::Pause),
            "draining" => Ok(Self::Draining),
            _ => Err(anyhow::anyhow!("Unknown scheduling state '{s}'")),
        }
    }
}

impl From<NodeSchedulingPolicy> for String {
    fn from(value: NodeSchedulingPolicy) -> String {
        use NodeSchedulingPolicy::*;
        match value {
            Active => "active",
            Filling => "filling",
            Pause => "pause",
            Draining => "draining",
        }
        .to_string()
    }
}

/// Controls how tenant shards are mapped to locations on pageservers, e.g. whether
/// to create secondary locations.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum PlacementPolicy {
    /// Cheapest way to attach a tenant: just one pageserver, no secondary
    Single,
    /// Production-ready way to attach a tenant: one attached pageserver and
    /// some number of secondaries.
    Double(usize),
    /// Create one secondary mode locations. This is useful when onboarding
    /// a tenant, or for an idle tenant that we might want to bring online quickly.
    Secondary,

    /// Do not attach to any pageservers.  This is appropriate for tenants that
    /// have been idle for a long time, where we do not mind some delay in making
    /// them available in future.
    Detached,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantShardMigrateResponse {}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json;

    /// Check stability of PlacementPolicy's serialization
    #[test]
    fn placement_policy_encoding() -> anyhow::Result<()> {
        let v = PlacementPolicy::Double(1);
        let encoded = serde_json::to_string(&v)?;
        assert_eq!(encoded, "{\"Double\":1}");
        assert_eq!(serde_json::from_str::<PlacementPolicy>(&encoded)?, v);

        let v = PlacementPolicy::Single;
        let encoded = serde_json::to_string(&v)?;
        assert_eq!(encoded, "\"Single\"");
        assert_eq!(serde_json::from_str::<PlacementPolicy>(&encoded)?, v);
        Ok(())
    }
}
