use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utils::id::{EndpointId, TenantId, TimelineId};

/// Claims to add, remove, or retrieve endpoint data. Used by compute_ctl
#[derive(Deserialize, Serialize, PartialEq)]
pub struct EndpointStorageClaims {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub endpoint_id: EndpointId,
    pub exp: u64,
}

/// Claims to remove tenant, timeline, or endpoint data. Used by control plane
#[derive(Deserialize, Serialize, PartialEq)]
pub struct DeletePrefixClaims {
    pub tenant_id: TenantId,
    /// None when tenant is deleted (endpoint_id is also None in this case)
    pub timeline_id: Option<TimelineId>,
    /// None when timeline is deleted
    pub endpoint_id: Option<EndpointId>,
    pub exp: u64,
}

impl Display for EndpointStorageClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EndpointClaims(tenant_id={} timeline_id={} endpoint_id={} exp={})",
            self.tenant_id, self.timeline_id, self.endpoint_id, self.exp
        )
    }
}

impl Display for DeletePrefixClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeletePrefixClaims(tenant_id={} timeline_id={} endpoint_id={}, exp={})",
            self.tenant_id,
            self.timeline_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or("".to_string()),
            self.endpoint_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or("".to_string()),
            self.exp
        )
    }
}
