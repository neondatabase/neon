use crate::walrecord::NeonWalRecord;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::AddAssign;
use std::time::Duration;

pub use pageserver_api::key::{Key, KEY_SIZE};

/// A 'value' stored for a one Key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
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

/// The maximum size of a value supported by the pageserver
pub const MAX_VALUE_SIZE: usize = 10_000_000;

#[cfg(test)]
mod test {
    use super::*;

    use bytes::Bytes;
    use utils::bin_ser::BeSer;

    macro_rules! roundtrip {
        ($orig:expr, $expected:expr) => {{
            let orig: Value = $orig;

            let actual = Value::ser(&orig).unwrap();
            let expected: &[u8] = &$expected;

            assert_eq!(utils::Hex(&actual), utils::Hex(expected));

            let deser = Value::des(&actual).unwrap();

            assert_eq!(orig, deser);
        }};
    }

    #[test]
    fn image_roundtrip() {
        let image = Bytes::from_static(b"foobar");
        let image = Value::Image(image);

        #[rustfmt::skip]
        let expected = [
            // top level discriminator of 4 bytes
            0x00, 0x00, 0x00, 0x00,
            // 8 byte length
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
            // foobar
            0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72
        ];

        roundtrip!(image, expected);
    }

    #[test]
    fn walrecord_postgres_roundtrip() {
        let rec = NeonWalRecord::Postgres {
            will_init: true,
            rec: Bytes::from_static(b"foobar"),
        };
        let rec = Value::WalRecord(rec);

        #[rustfmt::skip]
        let expected = [
            // flattened discriminator of total 8 bytes
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            // will_init
            0x01,
            // 8 byte length
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
            // foobar
            0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72
        ];

        roundtrip!(rec, expected);
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

    /// The layers which were garbage collected.
    ///
    /// Used in `/v1/tenant/:tenant_id/timeline/:timeline_id/do_gc` to wait for the layers to be
    /// dropped in tests.
    #[cfg(feature = "testing")]
    #[serde(skip)]
    pub(crate) doomed_layers: Vec<crate::tenant::storage_layer::Layer>,
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

        #[cfg(feature = "testing")]
        {
            let mut other = other;
            self.doomed_layers.append(&mut other.doomed_layers);
        }
    }
}
