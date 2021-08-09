use serde::{Deserialize, Serialize};

use crate::ZTenantId;

#[derive(Serialize, Deserialize)]
pub struct BranchCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    pub name: String,
    pub start_point: String,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
}
