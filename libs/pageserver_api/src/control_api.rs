/// Types in this file are for pageserver's upward-facing API calls to the control plane
use serde::{Deserialize, Serialize};
use utils::id::{NodeId, TenantId};

#[derive(Serialize, Deserialize)]
pub struct ReAttachRequest {
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct ReAttachResponseTenant {
    pub id: TenantId,
    pub generation: u32,
}

#[derive(Serialize, Deserialize)]
pub struct ReAttachResponse {
    pub tenants: Vec<ReAttachResponseTenant>,
}

#[derive(Serialize, Deserialize)]
pub struct ValidateRequestTenant {
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

#[derive(Serialize, Deserialize)]
pub struct ValidateResponseTenant {
    pub id: TenantId,
    pub valid: bool,
}

#[cfg(testing)]
#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    tenant_id: TenantId,
    pageserver_id: Option<NodeId>,
}

#[cfg(testing)]
#[derive(Serialize, Deserialize)]
pub struct AttachHookResponse {
    gen: Option<u32>,
}
