//!
//! Common utilities for dealing with PostgreSQL relation files.
//!
use once_cell::sync::OnceCell;
use regex::Regex;

//
// Fork numbers, from relpath.h
//
pub const MAIN_FORKNUM: u8 = 0;
pub const FSM_FORKNUM: u8 = 1;
pub const VISIBILITYMAP_FORKNUM: u8 = 2;
pub const INIT_FORKNUM: u8 = 3;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum FilePathError {
    #[error("invalid relation fork name")]
    InvalidForkName,
    #[error("invalid relation data file name")]
    InvalidFileName,
}

impl From<core::num::ParseIntError> for FilePathError {
    fn from(_e: core::num::ParseIntError) -> Self {
        FilePathError::InvalidFileName
    }
}

/// Convert Postgres relation file's fork suffix to fork number.
pub fn forkname_to_number(forkname: Option<&str>) -> Result<u8, FilePathError> {
    match forkname {
        // "main" is not in filenames, it's implicit if the fork name is not present
        None => Ok(MAIN_FORKNUM),
        Some("fsm") => Ok(FSM_FORKNUM),
        Some("vm") => Ok(VISIBILITYMAP_FORKNUM),
        Some("init") => Ok(INIT_FORKNUM),
        Some(_) => Err(FilePathError::InvalidForkName),
    }
}

/// Convert Postgres fork number to the right suffix of the relation data file.
pub fn forknumber_to_name(forknum: u8) -> Option<&'static str> {
    match forknum {
        MAIN_FORKNUM => None,
        FSM_FORKNUM => Some("fsm"),
        VISIBILITYMAP_FORKNUM => Some("vm"),
        INIT_FORKNUM => Some("init"),
        _ => Some("UNKNOWN FORKNUM"),
    }
}

/// Parse a filename of a relation file. Returns (relfilenode, forknum, segno) tuple.
///
/// Formats:
///
/// ```text
/// <oid>
/// <oid>_<fork name>
/// <oid>.<segment number>
/// <oid>_<fork name>.<segment number>
/// ```
///
/// See functions relpath() and _mdfd_segpath() in PostgreSQL sources.
///
pub fn parse_relfilename(fname: &str) -> Result<(u32, u8, u32), FilePathError> {
    static RELFILE_RE: OnceCell<Regex> = OnceCell::new();
    RELFILE_RE.get_or_init(|| {
        Regex::new(r"^(?P<relnode>\d+)(_(?P<forkname>[a-z]+))?(\.(?P<segno>\d+))?$").unwrap()
    });

    let caps = RELFILE_RE
        .get()
        .unwrap()
        .captures(fname)
        .ok_or(FilePathError::InvalidFileName)?;

    let relnode_str = caps.name("relnode").unwrap().as_str();
    let relnode = relnode_str.parse::<u32>()?;

    let forkname = caps.name("forkname").map(|f| f.as_str());
    let forknum = forkname_to_number(forkname)?;

    let segno_match = caps.name("segno");
    let segno = if segno_match.is_none() {
        0
    } else {
        segno_match.unwrap().as_str().parse::<u32>()?
    };

    Ok((relnode, forknum, segno))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_relfilenames() {
        assert_eq!(parse_relfilename("1234"), Ok((1234, 0, 0)));
        assert_eq!(parse_relfilename("1234_fsm"), Ok((1234, 1, 0)));
        assert_eq!(parse_relfilename("1234_vm"), Ok((1234, 2, 0)));
        assert_eq!(parse_relfilename("1234_init"), Ok((1234, 3, 0)));

        assert_eq!(parse_relfilename("1234.12"), Ok((1234, 0, 12)));
        assert_eq!(parse_relfilename("1234_fsm.12"), Ok((1234, 1, 12)));
        assert_eq!(parse_relfilename("1234_vm.12"), Ok((1234, 2, 12)));
        assert_eq!(parse_relfilename("1234_init.12"), Ok((1234, 3, 12)));

        // relfilenode is unsigned, so it can go up to 2^32-1
        assert_eq!(parse_relfilename("3147483648"), Ok((3147483648, 0, 0)));
    }

    #[test]
    fn test_parse_invalid_relfilenames() {
        assert_eq!(
            parse_relfilename("foo"),
            Err(FilePathError::InvalidFileName)
        );
        assert_eq!(
            parse_relfilename("1.2.3"),
            Err(FilePathError::InvalidFileName)
        );
        assert_eq!(
            parse_relfilename("1234_invalid"),
            Err(FilePathError::InvalidForkName)
        );
        assert_eq!(
            parse_relfilename("1234_"),
            Err(FilePathError::InvalidFileName)
        );

        // too large for u32
        assert_eq!(
            parse_relfilename("12345678901"),
            Err(FilePathError::InvalidFileName)
        );
        assert_eq!(
            parse_relfilename("-1234"),
            Err(FilePathError::InvalidFileName)
        );
    }

    #[test]
    fn test_parse_weird_relfilenames() {
        // we accept 0 for the relfilenode, but PostgreSQL should never do that.
        assert_eq!(parse_relfilename("0"), Ok((0, 0, 0)));

        // PostgreSQL has a limit of 2^32-2 blocks in a table. With 8k block size and
        // 1 GB segments, the max segment number is 32767. But we accept larger values
        // currently.
        assert_eq!(parse_relfilename("1.123456"), Ok((1, 0, 123456)));
    }
}
