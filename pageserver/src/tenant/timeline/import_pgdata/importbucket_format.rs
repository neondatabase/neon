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
