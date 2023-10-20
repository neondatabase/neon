use crate::walrecord::NeonWalRecord;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::{AddAssign, Range};
use std::time::Duration;

pub use pageserver_api::key::{Key, KEY_SIZE};

pub fn key_range_size(key_range: &Range<Key>) -> u32 {
    let start = key_range.start;
    let end = key_range.end;

    if end.field1 != start.field1
        || end.field2 != start.field2
        || end.field3 != start.field3
        || end.field4 != start.field4
    {
        return u32::MAX;
    }

    let start = (start.field5 as u64) << 32 | start.field6 as u64;
    let end = (end.field5 as u64) << 32 | end.field6 as u64;

    let diff = end - start;
    if diff > u32::MAX as u64 {
        u32::MAX
    } else {
        diff as u32
    }
}

pub fn singleton_range(key: Key) -> Range<Key> {
    key..key.next()
}

/// A 'value' stored for a one Key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// An Image value contains a full copy of the value
    Image(Bytes),
    /// A WalRecord value contains a WAL record that needs to be
    /// replayed get the full value. Replaying the WAL record
    /// might need a previous version of the value (if will_init()
    /// returns false), or it may be replayed stand-alone (true).
    WalRecord(NeonWalRecord),
}

impl Value {
    pub fn is_image(&self) -> bool {
        matches!(self, Value::Image(_))
    }

    pub fn will_init(&self) -> bool {
        match self {
            Value::Image(_) => true,
            Value::WalRecord(rec) => rec.will_init(),
        }
    }
}

///
/// Result of performing GC
///
#[derive(Default, Serialize, Debug)]
pub struct GcResult {
    pub layers_total: u64,
    pub layers_needed_by_cutoff: u64,
    pub layers_needed_by_pitr: u64,
    pub layers_needed_by_branches: u64,
    pub layers_not_updated: u64,
    pub layers_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.

    #[serde(serialize_with = "serialize_duration_as_millis")]
    pub elapsed: Duration,
}

// helper function for `GcResult`, serializing a `Duration` as an integer number of milliseconds
fn serialize_duration_as_millis<S>(d: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    d.as_millis().serialize(serializer)
}

impl AddAssign for GcResult {
    fn add_assign(&mut self, other: Self) {
        self.layers_total += other.layers_total;
        self.layers_needed_by_pitr += other.layers_needed_by_pitr;
        self.layers_needed_by_cutoff += other.layers_needed_by_cutoff;
        self.layers_needed_by_branches += other.layers_needed_by_branches;
        self.layers_not_updated += other.layers_not_updated;
        self.layers_removed += other.layers_removed;

        self.elapsed += other.elapsed;
    }
}
