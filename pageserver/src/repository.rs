use crate::walrecord::NeonWalRecord;
use anyhow::{bail, Result};
use byteorder::{ByteOrder, BE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{AddAssign, Range};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
/// Key used in the Repository kv-store.
///
/// The Repository treats this as an opaque struct, but see the code in pgdatadir_mapping.rs
/// for what we actually store in these fields.
pub struct Key {
    pub field1: u8,
    pub field2: u32,
    pub field3: u32,
    pub field4: u32,
    pub field5: u8,
    pub field6: u32,
}

pub const KEY_SIZE: usize = 18;

impl Key {
    pub fn next(&self) -> Key {
        self.add(1)
    }

    pub fn add(&self, x: u32) -> Key {
        let mut key = *self;

        let r = key.field6.overflowing_add(x);
        key.field6 = r.0;
        if r.1 {
            let r = key.field5.overflowing_add(1);
            key.field5 = r.0;
            if r.1 {
                let r = key.field4.overflowing_add(1);
                key.field4 = r.0;
                if r.1 {
                    let r = key.field3.overflowing_add(1);
                    key.field3 = r.0;
                    if r.1 {
                        let r = key.field2.overflowing_add(1);
                        key.field2 = r.0;
                        if r.1 {
                            let r = key.field1.overflowing_add(1);
                            key.field1 = r.0;
                            assert!(!r.1);
                        }
                    }
                }
            }
        }
        key
    }

    pub fn from_slice(b: &[u8]) -> Self {
        Key {
            field1: b[0],
            field2: u32::from_be_bytes(b[1..5].try_into().unwrap()),
            field3: u32::from_be_bytes(b[5..9].try_into().unwrap()),
            field4: u32::from_be_bytes(b[9..13].try_into().unwrap()),
            field5: b[13],
            field6: u32::from_be_bytes(b[14..18].try_into().unwrap()),
        }
    }

    pub fn write_to_byte_slice(&self, buf: &mut [u8]) {
        buf[0] = self.field1;
        BE::write_u32(&mut buf[1..5], self.field2);
        BE::write_u32(&mut buf[5..9], self.field3);
        BE::write_u32(&mut buf[9..13], self.field4);
        buf[13] = self.field5;
        BE::write_u32(&mut buf[14..18], self.field6);
    }
}

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

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02X}{:08X}{:08X}{:08X}{:02X}{:08X}",
            self.field1, self.field2, self.field3, self.field4, self.field5, self.field6
        )
    }
}

impl Key {
    pub const MIN: Key = Key {
        field1: u8::MIN,
        field2: u32::MIN,
        field3: u32::MIN,
        field4: u32::MIN,
        field5: u8::MIN,
        field6: u32::MIN,
    };
    pub const MAX: Key = Key {
        field1: u8::MAX,
        field2: u32::MAX,
        field3: u32::MAX,
        field4: u32::MAX,
        field5: u8::MAX,
        field6: u32::MAX,
    };

    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 36 {
            bail!("parse error");
        }
        Ok(Key {
            field1: u8::from_str_radix(&s[0..2], 16)?,
            field2: u32::from_str_radix(&s[2..10], 16)?,
            field3: u32::from_str_radix(&s[10..18], 16)?,
            field4: u32::from_str_radix(&s[18..26], 16)?,
            field5: u8::from_str_radix(&s[26..28], 16)?,
            field6: u32::from_str_radix(&s[28..36], 16)?,
        })
    }
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
#[derive(Default)]
pub struct GcResult {
    pub layers_total: u64,
    pub layers_needed_by_cutoff: u64,
    pub layers_needed_by_pitr: u64,
    pub layers_needed_by_branches: u64,
    pub layers_not_updated: u64,
    pub layers_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.

    pub elapsed: Duration,
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
