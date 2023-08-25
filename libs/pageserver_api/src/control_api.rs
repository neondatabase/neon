/// Types in this file are for pageserver's upward-facing API calls to the control plane
use serde::{Deserialize, Serialize};
use utils::id::{NodeId, TenantId};

#[derive(Serialize, Deserialize)]
struct ReAttachRequest {
    node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
struct ReAttachResponseTenant {
    id: TenantId,
    generation: u32,
}

#[derive(Serialize, Deserialize)]
struct ReAttachResponse {
    tenants: Vec<ReAttachResponseTenant>,
}

#[derive(Serialize, Deserialize)]
struct ValidateRequestTenant {
    id: TenantId,
    gen: u32,
}

#[derive(Serialize, Deserialize)]
struct ValidateRequest {
    tenants: Vec<ValidateRequestTenant>,
}

#[derive(Serialize, Deserialize)]
struct ValidateResponse {
    tenants: Vec<ValidateResponseTenant>,
}

#[derive(Serialize, Deserialize)]
struct ValidateResponseTenant {
    id: TenantId,
    valid: bool,
}
