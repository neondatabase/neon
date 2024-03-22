//! Types in this file are for pageserver's upward-facing API calls to the control plane,
//! required for acquiring and validating tenant generation numbers.
//!
//! See docs/rfcs/025-generation-numbers.md

use serde::{Deserialize, Serialize};
use utils::id::NodeId;

use crate::{
    controller_api::NodeRegisterRequest, models::LocationConfigMode, shard::TenantShardId,
};

/// Upcall message sent by the pageserver to the configured `control_plane_api` on
/// startup.
#[derive(Serialize, Deserialize)]
pub struct ReAttachRequest {
    pub node_id: NodeId,

    /// Optional inline self-registration: this is useful with the storage controller,
    /// if the node already has a node_id set.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub register: Option<NodeRegisterRequest>,
}

fn default_mode() -> LocationConfigMode {
    LocationConfigMode::AttachedSingle
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReAttachResponseTenant {
    pub id: TenantShardId,
    /// Mandatory if LocationConfigMode is None or set to an Attached* mode
    pub gen: Option<u32>,

    /// Default value only for backward compat: this field should be set
    #[serde(default = "default_mode")]
    pub mode: LocationConfigMode,
}
#[derive(Serialize, Deserialize)]
pub struct ReAttachResponse {
    pub tenants: Vec<ReAttachResponseTenant>,
}

#[derive(Serialize, Deserialize)]
pub struct ValidateRequestTenant {
    pub id: TenantShardId,
    pub gen: u32,
}

#[derive(Serialize, Deserialize)]
pub struct ValidateRequest {
    pub tenants: Vec<ValidateRequestTenant>,
}

#[derive(Serialize, Deserialize)]
pub struct ValidateResponse {
    pub tenants: Vec<ValidateResponseTenant>,
}

#[derive(Serialize, Deserialize)]
pub struct ValidateResponseTenant {
    pub id: TenantShardId,
    pub valid: bool,
}
