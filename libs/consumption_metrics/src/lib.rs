//!
//! Shared code for consumption metics collection
//!
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::Serialize;

#[derive(Serialize, Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[serde(tag = "type")]
pub enum EventType {
    #[serde(rename = "absolute")]
    Absolute { time: DateTime<Utc> },
    #[serde(rename = "incremental")]
    Incremental {
        start_time: DateTime<Utc>,
        stop_time: DateTime<Utc>,
    },
}

impl EventType {
    pub fn absolute_time(&self) -> Option<&DateTime<Utc>> {
        use EventType::*;
        match self {
            Absolute { time } => Some(time),
            _ => None,
        }
    }

    pub fn incremental_timerange(&self) -> Option<std::ops::Range<&DateTime<Utc>>> {
        // these can most likely be thought of as Range or RangeFull
        use EventType::*;
        match self {
            Incremental {
                start_time,
                stop_time,
            } => Some(start_time..stop_time),
            _ => None,
        }
    }

    pub fn is_incremental(&self) -> bool {
        matches!(self, EventType::Incremental { .. })
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Event<Extra, Metric: Serialize> {
    #[serde(flatten)]
    #[serde(rename = "type")]
    pub kind: EventType,

    pub metric: Metric,
    pub idempotency_key: String,
    pub value: u64,

    #[serde(flatten)]
    pub extra: Extra,
}

pub fn idempotency_key(node_id: &str) -> String {
    IdempotencyKey::generate(node_id).to_string()
}

/// Downstream users will use these to detect upload retries.
pub struct IdempotencyKey<'a> {
    now: chrono::DateTime<Utc>,
    node_id: &'a str,
    nonce: u16,
}

impl std::fmt::Display for IdempotencyKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{:04}", self.now, self.node_id, self.nonce)
    }
}

impl<'a> IdempotencyKey<'a> {
    pub fn generate(node_id: &'a str) -> Self {
        IdempotencyKey {
            now: Utc::now(),
            node_id,
            nonce: rand::thread_rng().gen_range(0..=9999),
        }
    }
}

pub const CHUNK_SIZE: usize = 1000;

// Just a wrapper around a slice of events
// to serialize it as `{"events" : [ ] }
#[derive(serde::Serialize)]
pub struct EventChunk<'a, T: Clone> {
    pub events: std::borrow::Cow<'a, [T]>,
}
