//! Low level Postgres protocol APIs.
//!
//! This crate implements the low level components of Postgres's communication
//! protocol, including message and value serialization and deserialization.
//! It is designed to be used as a building block by higher level APIs such as
//! `rust-postgres`, and should not typically be used directly.
//!
//! # Note
//!
//! This library assumes that the `client_encoding` backend parameter has been
//! set to `UTF8`. It will most likely not behave properly if that is not the case.
#![warn(missing_docs, clippy::all)]

use std::{ffi::CStr, io};

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};

pub mod authentication;
pub mod escape;
pub mod message;
pub mod password;
pub mod types;

/// A Postgres OID.
pub type Oid = u32;

/// A Postgres Log Sequence Number (LSN).
pub type Lsn = u64;

/// An enum indicating if a value is `NULL` or not.
pub enum IsNull {
    /// The value is `NULL`.
    Yes,
    /// The value is not `NULL`.
    No,
}

/// A [`std::ffi::CStr`] but without the null byte.
#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct CSafeStr([u8]);

impl CSafeStr {
    /// Create a new `CSafeStr`, erroring if the bytes contains a null.
    pub fn new(bytes: &[u8]) -> Result<&Self, io::Error> {
        let nul_pos = memchr::memchr(0, bytes);
        match nul_pos {
            Some(nul_pos) => Err(io::Error::other(format!(
                "unexpected null byte at position {nul_pos}"
            ))),
            None => {
                // Safety: CSafeStr is transparent over [u8].
                Ok(unsafe { std::mem::transmute(bytes) })
            }
        }
    }

    /// Create a new `CSafeStr` up until the next null.
    pub fn take<'a>(bytes: &mut &'a [u8]) -> &'a Self {
        let nul_pos = memchr::memchr(0, bytes).unwrap_or(bytes.len());
        let bytes = bytes
            .split_off(..nul_pos)
            .expect("nul_pos should be in-bounds");
        // Safety: CSafeStr is transparent over [u8].
        unsafe { std::mem::transmute(bytes) }
    }

    /// Get the bytes of this CSafeStr.
    pub const fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Create a new `CSafeStr`
    pub const fn from_cstr(s: &CStr) -> &CSafeStr {
        // Safety: CSafeStr is transparent over [u8].
        unsafe { std::mem::transmute(s.to_bytes()) }
    }
}

impl<'a> From<&'a CStr> for &'a CSafeStr {
    fn from(s: &'a CStr) -> &'a CSafeStr {
        CSafeStr::from_cstr(s)
    }
}

fn write_nullable<F>(serializer: F, buf: &mut BytesMut)
where
    F: FnOnce(&mut BytesMut) -> IsNull,
{
    let base = buf.len();
    buf.put_i32(0);
    let size = match serializer(buf) {
        // this is an unreasonable enough case that I think a panic is acceptable.
        IsNull::No => i32::from_usize(buf.len() - base - 4)
            .expect("buffer size should not be larger than i32::MAX"),
        IsNull::Yes => -1,
    };
    BigEndian::write_i32(&mut buf[base..], size);
}

trait FromUsize: Sized {
    fn from_usize(x: usize) -> Result<Self, io::Error>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            #[inline]
            fn from_usize(x: usize) -> io::Result<$t> {
                if x > <$t>::MAX as usize {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large to transmit",
                    ))
                } else {
                    Ok(x as $t)
                }
            }
        }
    };
}

from_usize!(i16);
from_usize!(i32);
