use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, AddAssign};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Transaction log block size in bytes
pub const XLOG_BLCKSZ: u32 = 8192;

/// A Postgres LSN (Log Sequence Number), also known as an XLogRecPtr
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Lsn(pub u64);

/// We tried to parse an LSN from a string, but failed
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("LsnParseError")]
pub struct LsnParseError;

impl Lsn {
    /// Maximum possible value for an LSN
    pub const MAX: Lsn = Lsn(u64::MAX);

    /// Invalid value for InvalidXLogRecPtr, as defined in xlogdefs.h
    pub const INVALID: Lsn = Lsn(0);

    /// Subtract a number, returning None on overflow.
    pub fn checked_sub<T: Into<u64>>(self, other: T) -> Option<Lsn> {
        let other: u64 = other.into();
        self.0.checked_sub(other).map(Lsn)
    }

    /// Subtract a number, returning the difference as i128 to avoid overflow.
    pub fn widening_sub<T: Into<u64>>(self, other: T) -> i128 {
        let other: u64 = other.into();
        i128::from(self.0) - i128::from(other)
    }

    /// Parse an LSN from a filename in the form `0000000000000000`
    pub fn from_filename<F>(filename: F) -> Result<Self, LsnParseError>
    where
        F: AsRef<Path>,
    {
        let filename: &Path = filename.as_ref();
        let filename = filename.to_str().ok_or(LsnParseError)?;
        Lsn::from_hex(filename)
    }

    /// Parse an LSN from a string in the form `0000000000000000`
    pub fn from_hex<S>(s: S) -> Result<Self, LsnParseError>
    where
        S: AsRef<str>,
    {
        let s: &str = s.as_ref();
        let n = u64::from_str_radix(s, 16).or(Err(LsnParseError))?;
        Ok(Lsn(n))
    }

    /// Compute the offset into a segment
    pub fn segment_offset(self, seg_sz: usize) -> usize {
        (self.0 % seg_sz as u64) as usize
    }

    /// Compute LSN of the segment start.
    pub fn segment_lsn(self, seg_sz: usize) -> Lsn {
        Lsn(self.0 - (self.0 % seg_sz as u64))
    }

    /// Compute the segment number
    pub fn segment_number(self, seg_sz: usize) -> u64 {
        self.0 / seg_sz as u64
    }

    /// Compute the offset into a block
    pub fn block_offset(self) -> u64 {
        const BLCKSZ: u64 = XLOG_BLCKSZ as u64;
        self.0 % BLCKSZ
    }

    /// Compute the bytes remaining in this block
    ///
    /// If the LSN is already at the block boundary, it will return `XLOG_BLCKSZ`.
    pub fn remaining_in_block(self) -> u64 {
        const BLCKSZ: u64 = XLOG_BLCKSZ as u64;
        BLCKSZ - (self.0 % BLCKSZ)
    }

    /// Compute the bytes remaining to fill a chunk of some size
    ///
    /// If the LSN is already at the chunk boundary, it will return 0.
    pub fn calc_padding<T: Into<u64>>(self, sz: T) -> u64 {
        let sz: u64 = sz.into();
        // By using wrapping_sub, we can subtract first and then mod second.
        // If it's done the other way around, then we would return a full
        // chunk size if we're already at the chunk boundary.
        // (Regular subtraction will panic on overflow in debug builds.)
        (sz.wrapping_sub(self.0)) % sz
    }

    /// Align LSN on 8-byte boundary (alignment of WAL records).
    pub fn align(&self) -> Lsn {
        Lsn((self.0 + 7) & !7)
    }

    /// Align LSN on 8-byte boundary (alignment of WAL records).
    pub fn is_aligned(&self) -> bool {
        *self == self.align()
    }

    /// Return if the LSN is valid
    /// mimics postgres XLogRecPtrIsInvalid macro
    pub fn is_valid(self) -> bool {
        self != Lsn::INVALID
    }
}

impl From<u64> for Lsn {
    fn from(n: u64) -> Self {
        Lsn(n)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> u64 {
        lsn.0
    }
}

impl FromStr for Lsn {
    type Err = LsnParseError;

    /// Parse an LSN from a string in the form `00000000/00000000`
    ///
    /// If the input string is missing the '/' character, then use `Lsn::from_hex`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splitter = s.trim().split('/');
        if let (Some(left), Some(right), None) = (splitter.next(), splitter.next(), splitter.next())
        {
            let left_num = u32::from_str_radix(left, 16).map_err(|_| LsnParseError)?;
            let right_num = u32::from_str_radix(right, 16).map_err(|_| LsnParseError)?;
            Ok(Lsn((left_num as u64) << 32 | right_num as u64))
        } else {
            Err(LsnParseError)
        }
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl Add<u64> for Lsn {
    type Output = Lsn;

    fn add(self, other: u64) -> Self::Output {
        // panic if the addition overflows.
        Lsn(self.0.checked_add(other).unwrap())
    }
}

impl AddAssign<u64> for Lsn {
    fn add_assign(&mut self, other: u64) {
        // panic if the addition overflows.
        self.0 = self.0.checked_add(other).unwrap();
    }
}

/// An [`Lsn`] that can be accessed atomically.
pub struct AtomicLsn {
    inner: AtomicU64,
}

impl AtomicLsn {
    /// Creates a new atomic `Lsn`.
    pub fn new(val: u64) -> Self {
        AtomicLsn {
            inner: AtomicU64::new(val),
        }
    }

    /// Atomically retrieve the `Lsn` value from memory.
    pub fn load(&self) -> Lsn {
        Lsn(self.inner.load(Ordering::Acquire))
    }

    /// Atomically store a new `Lsn` value to memory.
    pub fn store(&self, lsn: Lsn) {
        self.inner.store(lsn.0, Ordering::Release);
    }

    /// Adds to the current value, returning the previous value.
    ///
    /// This operation will panic on overflow.
    pub fn fetch_add(&self, val: u64) -> Lsn {
        let prev = self.inner.fetch_add(val, Ordering::AcqRel);
        assert!(prev.checked_add(val).is_some(), "AtomicLsn overflow");
        Lsn(prev)
    }

    /// Atomically sets the Lsn to the max of old and new value, returning the old value.
    pub fn fetch_max(&self, lsn: Lsn) -> Lsn {
        let prev = self.inner.fetch_max(lsn.0, Ordering::AcqRel);
        Lsn(prev)
    }
}

impl From<Lsn> for AtomicLsn {
    fn from(lsn: Lsn) -> Self {
        Self::new(lsn.0)
    }
}

/// Pair of LSN's pointing to the end of the last valid record and previous one
#[derive(Debug, Clone, Copy)]
pub struct RecordLsn {
    /// LSN at the end of the current record
    pub last: Lsn,
    /// LSN at the end of the previous record
    pub prev: Lsn,
}
