//! Utilities for binary serialization/deserialization.
//!
//! The [`BeSer`] trait allows us to define data structures
//! that can match data structures that are sent over the wire
//! in big-endian form with no packing.
//!
//! The [`LeSer`] trait does the same thing, in little-endian form.
//!
//! Note: you will get a compile error if you try to `use` both traits
//! in the same module or scope. This is intended to be a safety
//! mechanism: mixing big-endian and little-endian encoding in the same file
//! is error-prone.

#![warn(missing_docs)]

use bincode::Options;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Read, Write};
use thiserror::Error;

/// An error that occurred during a deserialize operation
///
/// This could happen because the input data was too short,
/// or because an invalid value was encountered.
#[derive(Debug, Error)]
pub enum DeserializeError {
    /// The deserializer isn't able to deserialize the supplied data.
    #[error("deserialize error")]
    BadInput,
    /// While deserializing from a `Read` source, an `io::Error` occurred.
    #[error("deserialize error: {0}")]
    Io(io::Error),
}

impl From<bincode::Error> for DeserializeError {
    fn from(e: bincode::Error) -> Self {
        match *e {
            bincode::ErrorKind::Io(io_err) => DeserializeError::Io(io_err),
            _ => DeserializeError::BadInput,
        }
    }
}

/// An error that occurred during a serialize operation
///
/// This probably means our [`Write`] failed, e.g. we tried
/// to write beyond the end of a buffer.
#[derive(Debug, Error)]
pub enum SerializeError {
    /// The serializer isn't able to serialize the supplied data.
    #[error("serialize error")]
    BadInput,
    /// While serializing into a `Write` sink, an `io::Error` occurred.
    #[error("serialize error: {0}")]
    Io(io::Error),
}

impl From<bincode::Error> for SerializeError {
    fn from(e: bincode::Error) -> Self {
        match *e {
            bincode::ErrorKind::Io(io_err) => SerializeError::Io(io_err),
            _ => SerializeError::BadInput,
        }
    }
}

/// A shortcut that configures big-endian binary serialization
///
/// Properties:
/// - Big endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
///
/// Does not allow trailing bytes in deserialization. If this is desired, you
/// may set [`Options::allow_trailing_bytes`] to explicitly accommodate this.
pub fn be_coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
}

/// A shortcut that configures little-ending binary serialization
///
/// Properties:
/// - Little endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
///
/// Does not allow trailing bytes in deserialization. If this is desired, you
/// may set [`Options::allow_trailing_bytes`] to explicitly accommodate this.
pub fn le_coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
}

/// Binary serialize/deserialize helper functions (Big Endian)
///
pub trait BeSer {
    /// Serialize into a byte slice
    fn ser_into_slice(&self, mut b: &mut [u8]) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        // &mut [u8] implements Write, but `ser_into` needs a mutable
        // reference to that. So we need the slightly awkward "mutable
        // reference to a mutable reference.
        self.ser_into(&mut b)
    }

    /// Serialize into a borrowed writer
    ///
    /// This is useful for most `Write` types except `&mut [u8]`, which
    /// can more easily use [`ser_into_slice`](Self::ser_into_slice).
    fn ser_into<W: Write>(&self, w: &mut W) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        be_coder().serialize_into(w, &self).map_err(|e| e.into())
    }

    /// Serialize into a new heap-allocated buffer
    fn ser(&self) -> Result<Vec<u8>, SerializeError>
    where
        Self: Serialize,
    {
        be_coder().serialize(&self).map_err(|e| e.into())
    }

    /// Deserialize from the full contents of a byte slice
    ///
    /// See also: [`BeSer::des_prefix`]
    fn des(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        be_coder()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a prefix of the byte slice
    ///
    /// Uses as much of the byte slice as is necessary to deserialize the
    /// type, but does not guarantee that the entire slice is used.
    ///
    /// See also: [`BeSer::des`]
    fn des_prefix(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        be_coder()
            .allow_trailing_bytes()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a reader
    fn des_from<R: Read>(r: &mut R) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        be_coder().deserialize_from(r).map_err(|e| e.into())
    }

    /// Compute the serialized size of a data structure
    ///
    /// Note: it may be faster to serialize to a buffer and then measure the
    /// buffer length, than to call `serialized_size` and then `ser_into`.
    fn serialized_size(&self) -> Result<u64, SerializeError>
    where
        Self: Serialize,
    {
        be_coder().serialized_size(self).map_err(|e| e.into())
    }
}

/// Binary serialize/deserialize helper functions (Little Endian)
///
pub trait LeSer {
    /// Serialize into a byte slice
    fn ser_into_slice(&self, mut b: &mut [u8]) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        // &mut [u8] implements Write, but `ser_into` needs a mutable
        // reference to that. So we need the slightly awkward "mutable
        // reference to a mutable reference.
        self.ser_into(&mut b)
    }

    /// Serialize into a borrowed writer
    ///
    /// This is useful for most `Write` types except `&mut [u8]`, which
    /// can more easily use [`ser_into_slice`](Self::ser_into_slice).
    fn ser_into<W: Write>(&self, w: &mut W) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        le_coder().serialize_into(w, &self).map_err(|e| e.into())
    }

    /// Serialize into a new heap-allocated buffer
    fn ser(&self) -> Result<Vec<u8>, SerializeError>
    where
        Self: Serialize,
    {
        le_coder().serialize(&self).map_err(|e| e.into())
    }

    /// Deserialize from the full contents of a byte slice
    ///
    /// See also: [`LeSer::des_prefix`]
    fn des(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        le_coder()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a prefix of the byte slice
    ///
    /// Uses as much of the byte slice as is necessary to deserialize the
    /// type, but does not guarantee that the entire slice is used.
    ///
    /// See also: [`LeSer::des`]
    fn des_prefix(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        le_coder()
            .allow_trailing_bytes()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a reader
    fn des_from<R: Read>(r: &mut R) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        le_coder().deserialize_from(r).map_err(|e| e.into())
    }

    /// Compute the serialized size of a data structure
    ///
    /// Note: it may be faster to serialize to a buffer and then measure the
    /// buffer length, than to call `serialized_size` and then `ser_into`.
    fn serialized_size(&self) -> Result<u64, SerializeError>
    where
        Self: Serialize,
    {
        le_coder().serialized_size(self).map_err(|e| e.into())
    }
}

// Because usage of `BeSer` or `LeSer` can be done with *either* a Serialize or
// DeserializeOwned implementation, the blanket implementation has to be for every type.
impl<T> BeSer for T {}
impl<T> LeSer for T {}
