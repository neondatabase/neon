//! Types in this file are for pageserver's upward-facing API calls to the control plane,
//! required for acquiring and validating tenant generation numbers.
//!
//! See docs/rfcs/025-generation-numbers.md

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::id::{NodeId, TenantId};

#[derive(Serialize, Deserialize)]
pub struct ReAttachRequest {
    pub node_id: NodeId,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct ReAttachResponseTenant {
    #[serde_as(as = "DisplayFromStr")]
    pub id: TenantId,
    pub generation: u32,
}

#[derive(Serialize, Deserialize)]
pub struct ReAttachResponse {
    pub tenants: Vec<ReAttachResponseTenant>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct ValidateRequestTenant {
    #[serde_as(as = "DisplayFromStr")]
    pub id: TenantId,
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

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct ValidateResponseTenant {
    #[serde_as(as = "DisplayFromStr")]
    pub id: TenantId,
    pub valid: bool,
}
