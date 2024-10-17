use serde::{Deserialize, Serialize};

#[cfg(feature = "testing")]
use camino::Utf8PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Root {
    V1(V1),
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum V1 {
    InProgress(InProgress),
    Done(Done),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(s: String) -> Self {
        Self(s)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InProgress {
    pub idempotency_key: IdempotencyKey,
    pub location: Location,
    pub started_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Done {
    pub idempotency_key: IdempotencyKey,
    pub started_at: chrono::NaiveDateTime,
    pub finished_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Location {
    #[cfg(feature = "testing")]
    LocalFs { path: Utf8PathBuf },
    AwsS3 {
        region: String,
        bucket: String,
        key: String,
    },
}

impl Root {
    pub fn is_done(&self) -> bool {
        match self {
            Root::V1(v1) => match v1 {
                V1::Done(_) => true,
                V1::InProgress(_) => false,
            },
        }
    }
    pub fn idempotency_key(&self) -> &IdempotencyKey {
        match self {
            Root::V1(v1) => match v1 {
                V1::InProgress(in_progress) => &in_progress.idempotency_key,
                V1::Done(done) => &done.idempotency_key,
            },
        }
    }
}
