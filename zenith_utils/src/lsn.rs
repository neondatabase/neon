#![warn(missing_docs)]

use std::fmt;
use std::ops::{Add, AddAssign, Sub};
use std::path::Path;
use std::str::FromStr;

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

impl Sub<u64> for Lsn {
    type Output = Lsn;

    fn sub(self, other: u64) -> Self::Output {
        // panic if the subtraction overflows/underflows.
        Lsn(self.0.checked_sub(other).unwrap())
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

        assert_eq!(Lsn::from_hex("12345678AAAA5555"), Ok(Lsn(0x12345678AAAA5555)));
        assert_eq!(Lsn::from_hex("0"), Ok(Lsn(0)));
        assert_eq!(Lsn::from_hex("F12345678AAAA5555"), Err(LsnParseError));
    }
}
