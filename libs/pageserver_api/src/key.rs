use anyhow::{bail, Result};
use byteorder::{ByteOrder, BE};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::reltag::{BlockNumber, RelTag};

/// Key used in the Repository kv-store.
///
/// The Repository treats this as an opaque struct, but see the code in pgdatadir_mapping.rs
/// for what we actually store in these fields.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
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
    /// 'field2' is used to store tablespaceid for relations and small enum numbers for other relish.
    /// As long as Neon does not support tablespace (because of lack of access to local file system),
    /// we can assume that only some predefined namespace OIDs are used which can fit in u16
    pub fn to_i128(&self) -> i128 {
        assert!(self.field2 < 0xFFFF || self.field2 == 0xFFFFFFFF || self.field2 == 0x22222222);
        (((self.field1 & 0xf) as i128) << 120)
            | (((self.field2 & 0xFFFF) as i128) << 104)
            | ((self.field3 as i128) << 72)
            | ((self.field4 as i128) << 40)
            | ((self.field5 as i128) << 32)
            | self.field6 as i128
    }

    pub const fn from_i128(x: i128) -> Self {
        Key {
            field1: ((x >> 120) & 0xf) as u8,
            field2: ((x >> 104) & 0xFFFF) as u32,
            field3: (x >> 72) as u32,
            field4: (x >> 40) as u32,
            field5: (x >> 32) as u8,
            field6: x as u32,
        }
    }

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

#[inline(always)]
pub fn is_rel_block_key(key: &Key) -> bool {
    key.field1 == 0x00 && key.field4 != 0 && key.field6 != 0xffffffff
}

/// Guaranteed to return `Ok()` if [[is_rel_block_key]] returns `true` for `key`.
pub fn key_to_rel_block(key: Key) -> anyhow::Result<(RelTag, BlockNumber)> {
    Ok(match key.field1 {
        0x00 => (
            RelTag {
                spcnode: key.field2,
                dbnode: key.field3,
                relnode: key.field4,
                forknum: key.field5,
            },
            key.field6,
        ),
        _ => anyhow::bail!("unexpected value kind 0x{:02x}", key.field1),
    })
}

impl std::str::FromStr for Key {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::key::Key;

    use rand::Rng;
    use rand::SeedableRng;

    #[test]
    fn display_fromstr_bijection() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let key = Key {
            field1: rng.gen(),
            field2: rng.gen(),
            field3: rng.gen(),
            field4: rng.gen(),
            field5: rng.gen(),
            field6: rng.gen(),
        };

        assert_eq!(key, Key::from_str(&format!("{key}")).unwrap());
    }
}
