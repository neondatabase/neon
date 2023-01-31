use crate::walrecord::NeonWalRecord;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::AddAssign;
use std::time::Duration;

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
