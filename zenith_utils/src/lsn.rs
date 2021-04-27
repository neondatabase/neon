#![warn(missing_docs)]

use std::fmt;
use std::ops::{Add, AddAssign};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Transaction log block size in bytes
pub const XLOG_BLCKSZ: u32 = 8192;

/// A Postgres LSN (Log Sequence Number), also known as an XLogRecPtr
#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Lsn(pub u64);

/// We tried to parse an LSN from a string, but failed
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("LsnParseError")]
pub struct LsnParseError;

impl Lsn {
    /// Maximum possible value for an LSN
    pub const MAX: Lsn = Lsn(u64::MAX);

    /// Subtract a number, returning None on overflow.
    pub fn checked_sub<T: Into<u64>>(self, other: T) -> Option<Lsn> {
        let other: u64 = other.into();
        self.0.checked_sub(other).map(Lsn)
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
    pub fn segment_offset(self, seg_sz: u64) -> u64 {
        self.0 % seg_sz
    }

    /// Compute the segment number
    pub fn segment_number(self, seg_sz: u64) -> u64 {
        self.0 / seg_sz
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
        let mut splitter = s.split('/');
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
        if prev.checked_add(val).is_none() {
            panic!("AtomicLsn overflow");
        }
        Lsn(prev)
    }

    /// Atomically sets the Lsn to the max of old and new value, returning the old value.
    pub fn fetch_max(&self, lsn: Lsn) -> Lsn {
        let prev = self.inner.fetch_max(lsn.0, Ordering::AcqRel);
        Lsn(prev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_strings() {
        assert_eq!("12345678/AAAA5555".parse(), Ok(Lsn(0x12345678AAAA5555)));
        assert_eq!("aaaa/bbbb".parse(), Ok(Lsn(0x0000AAAA0000BBBB)));
        assert_eq!("1/A".parse(), Ok(Lsn(0x000000010000000A)));
        assert_eq!("0/0".parse(), Ok(Lsn(0)));
        "ABCDEFG/12345678".parse::<Lsn>().unwrap_err();
        "123456789/AAAA5555".parse::<Lsn>().unwrap_err();
        "12345678/AAAA55550".parse::<Lsn>().unwrap_err();
        "-1/0".parse::<Lsn>().unwrap_err();
        "1/-1".parse::<Lsn>().unwrap_err();

        assert_eq!(format!("{}", Lsn(0x12345678AAAA5555)), "12345678/AAAA5555");
        assert_eq!(format!("{}", Lsn(0x000000010000000A)), "1/A");

        assert_eq!(
            Lsn::from_hex("12345678AAAA5555"),
            Ok(Lsn(0x12345678AAAA5555))
        );
        assert_eq!(Lsn::from_hex("0"), Ok(Lsn(0)));
        assert_eq!(Lsn::from_hex("F12345678AAAA5555"), Err(LsnParseError));
    }

    #[test]
    fn test_lsn_math() {
        assert_eq!(Lsn(1234) + 11u64, Lsn(1245));

        assert_eq!(
            {
                let mut lsn = Lsn(1234);
                lsn += 11u64;
                lsn
            },
            Lsn(1245)
        );

        assert_eq!(Lsn(1234).checked_sub(1233u64), Some(Lsn(1)));
        assert_eq!(Lsn(1234).checked_sub(1235u64), None);

        let seg_sz = 16u64 * 1024 * 1024;
        assert_eq!(Lsn(0x1000007).segment_offset(seg_sz), 7u64);
        assert_eq!(Lsn(0x1000007).segment_number(seg_sz), 1u64);

        assert_eq!(Lsn(0x4007).block_offset(), 7u64);
        assert_eq!(Lsn(0x4000).block_offset(), 0u64);
        assert_eq!(Lsn(0x4007).remaining_in_block(), 8185u64);
        assert_eq!(Lsn(0x4000).remaining_in_block(), 8192u64);

        assert_eq!(Lsn(0xffff01).calc_padding(seg_sz), 255u64);
        assert_eq!(Lsn(0x2000000).calc_padding(seg_sz), 0u64);
        assert_eq!(Lsn(0xffff01).calc_padding(8u32), 7u64);
        assert_eq!(Lsn(0xffff00).calc_padding(8u32), 0u64);
    }

    #[test]
    fn test_atomic_lsn() {
        let lsn = AtomicLsn::new(0);
        assert_eq!(lsn.fetch_add(1234), Lsn(0));
        assert_eq!(lsn.load(), Lsn(1234));
        lsn.store(Lsn(5678));
        assert_eq!(lsn.load(), Lsn(5678));

        assert_eq!(lsn.fetch_max(Lsn(6000)), Lsn(5678));
        assert_eq!(lsn.fetch_max(Lsn(5000)), Lsn(6000));
    }
}
