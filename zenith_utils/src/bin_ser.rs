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
        be_coder().serialize_into(w, &self).or(Err(SerializeError))
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
        be_coder().deserialize_from(r).or(Err(DeserializeError))
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
    use std::io::Cursor;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct ShortStruct {
        a: u8,
        b: u32,
    }

    const SHORT1: ShortStruct = ShortStruct { a: 7, b: 65536 };
    const SHORT1_ENC_BE: &[u8] = &[7, 0, 1, 0, 0];
    const SHORT1_ENC_BE_TRAILING: &[u8] = &[7, 0, 1, 0, 0, 255, 255, 255];
    const SHORT1_ENC_LE: &[u8] = &[7, 0, 0, 1, 0];
    const SHORT1_ENC_LE_TRAILING: &[u8] = &[7, 0, 0, 1, 0, 255, 255, 255];

    const SHORT2: ShortStruct = ShortStruct {
        a: 8,
        b: 0x07030000,
    };
    const SHORT2_ENC_BE: &[u8] = &[8, 7, 3, 0, 0];
    const SHORT2_ENC_BE_TRAILING: &[u8] = &[8, 7, 3, 0, 0, 0xff, 0xff, 0xff];
    const SHORT2_ENC_LE: &[u8] = &[8, 0, 0, 3, 7];
    const SHORT2_ENC_LE_TRAILING: &[u8] = &[8, 0, 0, 3, 7, 0xff, 0xff, 0xff];

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct LongMsg {
        pub tag: u8,
        pub blockpos: u32,
        pub last_flush_position: u64,
        pub apply: u64,
        pub timestamp: i64,
        pub reply_requested: u8,
    }

    const LONG1: LongMsg = LongMsg {
        tag: 42,
        blockpos: 0x1000_2000,
        last_flush_position: 0x1234_2345_3456_4567,
        apply: 0x9876_5432_10FE_DCBA,
        timestamp: 0x7788_99AA_BBCC_DDFF,
        reply_requested: 1,
    };

    #[test]
    fn be_short() {
        use super::BeSer;

        let encoded = SHORT1.ser().unwrap();
        assert_eq!(encoded, SHORT1_ENC_BE);

        let decoded = ShortStruct::des(SHORT2_ENC_BE).unwrap();
        assert_eq!(decoded, SHORT2);

        // with trailing data
        let decoded = ShortStruct::des(SHORT2_ENC_BE_TRAILING).unwrap();
        assert_eq!(decoded, SHORT2);

        // serialize into a `Write` sink.
        let mut buf = Cursor::new(vec![0xFF; 8]);
        SHORT1.ser_into(&mut buf).unwrap();
        assert_eq!(buf.into_inner(), SHORT1_ENC_BE_TRAILING);

        // deserialize from a `Write` sink.
        let buf = Cursor::new(SHORT2_ENC_BE);
        let decoded = ShortStruct::des_from(buf).unwrap();
        assert_eq!(decoded, SHORT2);

        // deserialize from a `Write` sink that terminates early.
        let buf = Cursor::new([0u8; 4]);
        ShortStruct::des_from(buf).unwrap_err();
    }

    #[test]
    fn le_short() {
        use super::LeSer;

        let encoded = SHORT1.ser().unwrap();
        assert_eq!(encoded, SHORT1_ENC_LE);

        let decoded = ShortStruct::des(SHORT2_ENC_LE).unwrap();
        assert_eq!(decoded, SHORT2);

        // with trailing data
        let decoded = ShortStruct::des(SHORT2_ENC_LE_TRAILING).unwrap();
        assert_eq!(decoded, SHORT2);

        // serialize into a `Write` sink.
        let mut buf = Cursor::new(vec![0xFF; 8]);
        SHORT1.ser_into(&mut buf).unwrap();
        assert_eq!(buf.into_inner(), SHORT1_ENC_LE_TRAILING);

        // deserialize from a `Write` sink.
        let buf = Cursor::new(SHORT2_ENC_LE);
        let decoded = ShortStruct::des_from(buf).unwrap();
        assert_eq!(decoded, SHORT2);

        // deserialize from a `Write` sink that terminates early.
        let buf = Cursor::new([0u8; 4]);
        ShortStruct::des_from(buf).unwrap_err();
    }

    #[test]
    fn be_long() {
        use super::BeSer;

        let msg = LONG1;

        let encoded = msg.ser().unwrap();
        let expected = hex_literal::hex!(
            "2A 1000 2000 1234 2345 3456 4567 9876 5432 10FE DCBA 7788 99AA BBCC DDFF 01"
        );
        assert_eq!(encoded, expected);

        let msg2 = LongMsg::des(&encoded).unwrap();
        assert_eq!(msg, msg2);
    }

    #[test]
    fn le_long() {
        use super::LeSer;

        let msg = LONG1;

        let encoded = msg.ser().unwrap();
        let expected = hex_literal::hex!(
            "2A 0020 0010 6745 5634 4523 3412 BADC FE10 3254 7698 FFDD CCBB AA99 8877 01"
        );
        assert_eq!(encoded, expected);

        let msg2 = LongMsg::des(&encoded).unwrap();
        assert_eq!(msg, msg2);
    }
}
