use hex::FromHex;
use serde::{Deserialize, Serialize};
use utils::id::{NodeId, TenantId};

/// Types in this file are for pageserver's upward-facing API calls to the control plane,
/// required for acquiring and validating tenant generation numbers.
///
/// See docs/rfcs/025-generation-numbers.md

#[derive(Serialize, Deserialize)]
pub struct ReAttachRequest {
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct ReAttachResponseTenant {
    pub id: HexTenantId,
    pub generation: u32,
}

#[derive(Serialize, Deserialize)]
pub struct ReAttachResponse {
    pub tenants: Vec<ReAttachResponseTenant>,
}

#[derive(Serialize, Deserialize)]
pub struct ValidateRequestTenant {
    pub id: HexTenantId,
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
    pub id: HexTenantId,
    pub valid: bool,
}

/// Serialization helper: TenantId's serialization is an array of u8, which is rather unfriendly
/// for human readable encodings like JSON.
/// This class wraps it in serialization that is just the hex string representation,
/// which is more compact and readable in JSON.
#[derive(Eq, PartialEq, Clone, Hash)]
pub struct HexTenantId(TenantId);

impl HexTenantId {
    pub fn new(t: TenantId) -> Self {
        Self(t)
    }

    pub fn take(self) -> TenantId {
        self.0
    }
}

impl AsRef<TenantId> for HexTenantId {
    fn as_ref(&self) -> &TenantId {
        &self.0
    }
}

impl Serialize for HexTenantId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hex = self.0.hex_encode();
        serializer.collect_str(&hex)
    }
}

impl<'de> Deserialize<'de> for HexTenantId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        TenantId::from_hex(string)
            .map(HexTenantId::new)
            .map_err(|e| serde::de::Error::custom(format!("{e}")))
    }
}
