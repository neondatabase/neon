//! Utilities for binary serialization/deserialization.
//!
//! The [`BeSer`] trait allows us to define data structures
//! that can match data structures that are sent over the wire
//! in big-endian form with no packing.
//!
//! The [`LeSer`] trait does the same thing, in little-endian form.
//!
//! Note: you will get a compile error if you try to `use` both trais
//! in the same module or scope. This is intended to be a safety
//! mechanism: mixing big-endian and little-endian encoding in the same file
//! is error-prone.

#![warn(missing_docs)]

use bincode::Options;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{Read, Write};
use thiserror::Error;

/// An error that occurred during a deserialize operation
///
/// This could happen because the input data was too short,
/// or because an invalid value was encountered.
#[derive(Debug, Error)]
#[error("deserialize error")]
pub struct DeserializeError;

/// An error that occurred during a serialize operation
///
/// This probably means our [`Write`] failed, e.g. we tried
/// to write beyond the end of a buffer.
#[derive(Debug, Error)]
#[error("serialize error")]
pub struct SerializeError;

/// A shortcut that configures big-endian binary serialization
///
/// Properties:
/// - Big endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
/// - Allow trailing bytes: this means we don't throw an error
///   if the deserializer is passed a buffer with more data
///   past the end.
pub fn be_coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

/// A shortcut that configures little-ending binary serialization
///
/// Properties:
/// - Little endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
/// - Allow trailing bytes: this means we don't throw an error
///   if the deserializer is passed a buffer with more data
///   past the end.
pub fn le_coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

/// Binary serialize/deserialize helper functions (Big Endian)
///
pub trait BeSer: Serialize + DeserializeOwned {
    /// Serialize into a byte slice
    fn ser_into_slice<W: Write>(&self, b: &mut [u8]) -> Result<(), SerializeError> {
        // This is slightly awkward; we need a mutable reference to a mutable reference.
        let mut w = b;
        self.ser_into(&mut w)
    }

    /// Serialize into a borrowed writer
    ///
    /// This is useful for most `Write` types except `&mut [u8]`, which
    /// can more easily use [`ser_into_slice`](Self::ser_into_slice).
    fn ser_into<W: Write>(&self, w: &mut W) -> Result<(), SerializeError> {
        le_coder().serialize_into(w, &self).or(Err(SerializeError))
    }

    /// Serialize into a new heap-allocated buffer
    fn ser(&self) -> Result<Vec<u8>, SerializeError> {
        be_coder().serialize(&self).or(Err(SerializeError))
    }

    /// Deserialize from a byte slice
    fn des(buf: &[u8]) -> Result<Self, DeserializeError> {
        be_coder().deserialize(buf).or(Err(DeserializeError))
    }

    /// Deserialize from a reader
    ///
    /// tip: `&[u8]` implements `Read`
    fn des_from<R: Read>(r: R) -> Result<Self, DeserializeError> {
        le_coder().deserialize_from(r).or(Err(DeserializeError))
    }
}

/// Binary serialize/deserialize helper functions (Big Endian)
///
pub trait LeSer: Serialize + DeserializeOwned {
    /// Serialize into a byte slice
    fn ser_into_slice<W: Write>(&self, b: &mut [u8]) -> Result<(), SerializeError> {
        // This is slightly awkward; we need a mutable reference to a mutable reference.
        let mut w = b;
        self.ser_into(&mut w)
    }

    /// Serialize into a borrowed writer
    ///
    /// This is useful for most `Write` types except `&mut [u8]`, which
    /// can more easily use [`ser_into_slice`](Self::ser_into_slice).
    fn ser_into<W: Write>(&self, w: &mut W) -> Result<(), SerializeError> {
        le_coder().serialize_into(w, &self).or(Err(SerializeError))
    }

    /// Serialize into a new heap-allocated buffer
    fn ser(&self) -> Result<Vec<u8>, SerializeError> {
        le_coder().serialize(&self).or(Err(SerializeError))
    }

    /// Deserialize from a byte slice
    fn des(buf: &[u8]) -> Result<Self, DeserializeError> {
        le_coder().deserialize(buf).or(Err(DeserializeError))
    }

    /// Deserialize from a reader
    ///
    /// tip: `&[u8]` implements `Read`
    fn des_from<R: Read>(r: R) -> Result<Self, DeserializeError> {
        le_coder().deserialize_from(r).or(Err(DeserializeError))
    }
}

impl<T> BeSer for T where T: Serialize + DeserializeOwned {}

impl<T> LeSer for T where T: Serialize + DeserializeOwned {}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct ShortStruct {
        a: u8,
        b: u32,
    }

    #[test]
    fn be_short() {
        use super::BeSer;

        let x = ShortStruct { a: 7, b: 65536 };

        let encoded = x.ser().unwrap();

        assert_eq!(encoded, vec![7, 0, 1, 0, 0]);

        let raw = [8u8, 7, 3, 0, 0];
        let decoded = ShortStruct::des(&raw).unwrap();

        assert_eq!(
            decoded,
            ShortStruct {
                a: 8,
                b: 0x07030000
            }
        );

        // has trailing data
        let raw = [8u8, 7, 3, 0, 0, 0xFF, 0xFF, 0xFF];
        let _ = ShortStruct::des(&raw).unwrap();
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct BigMsg {
        pub tag: u8,
        pub blockpos: u64,
        pub last_flush_position: u64,
        pub apply: u64,
        pub timestamp: u64,
        pub reply_requested: u8,
    }

    #[test]
    fn be_big() {
        use super::BeSer;

        let msg = BigMsg {
            tag: 42,
            blockpos: 0x1000_2000_3000_4000,
            last_flush_position: 0x1234_2345_3456_4567,
            apply: 0x9876_5432_10FE_DCBA,
            timestamp: 0xABBA_CDDC_EFFE_0110,
            reply_requested: 1,
        };

        let encoded = msg.ser().unwrap();
        let expected = hex_literal::hex!(
            "2A 1000 2000 3000 4000 1234 2345 3456 4567 9876 5432 10FE DCBA ABBA CDDC EFFE 0110 01"
        );
        assert_eq!(encoded, expected);

        let msg2 = BigMsg::des(&encoded).unwrap();
        assert_eq!(msg, msg2);
    }
}
