use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct PgdataStatus {
    pub done: bool,
    // TODO: remaining fields
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardStatus {
    pub done: bool,
    // TODO: remaining fields
}

// TODO: dedupe with fast_import code
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub project_id: String,
    pub branch_id: String,
}
