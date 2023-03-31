use serde::{Serialize, Serializer};

use chrono::{DateTime, Utc};

use crate::compute::{ComputeState, ComputeStatus};

#[derive(Serialize, Debug)]
pub struct GenericAPIError {
    pub error: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ComputeStatusResponse {
    pub tenant: String,
    pub timeline: String,
    pub status: ComputeStatus,
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
}

impl From<ComputeState> for ComputeStatusResponse {
    fn from(state: ComputeState) -> Self {
        ComputeStatusResponse {
            tenant: state.tenant,
            timeline: state.timeline,
            status: state.status,
            last_active: state.last_active,
            error: state.error,
        }
    }
}

fn rfc3339_serialize<S>(x: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    x.to_rfc3339().serialize(s)
}
