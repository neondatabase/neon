//! Types in this file are for pageserver's upward-facing API calls to the control plane,
//! required for acquiring and validating tenant generation numbers.
//!
//! See docs/rfcs/025-generation-numbers.md

use serde::{Deserialize, Serialize};
use utils::id::NodeId;

use crate::shard::TenantShardId;

#[derive(Serialize, Deserialize)]
pub struct ReAttachRequest {
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct ReAttachResponseTenant {
    pub id: TenantShardId,
    pub gen: u32,
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
