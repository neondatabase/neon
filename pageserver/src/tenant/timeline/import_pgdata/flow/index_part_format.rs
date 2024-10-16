use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Root {
    V1(V1),
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum V1 {
    InProgress(InProgress),
    Done,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InProgress {
    pub s3_uri: String,
}
