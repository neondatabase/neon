///
/// Common utilities for dealing with PostgreSQL relation files.
///
use regex::Regex;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct FilePathError {
    msg: String,
}

impl Error for FilePathError {
    fn description(&self) -> &str {
        &self.msg
    }
}
impl FilePathError {
    pub fn new(msg: &str) -> FilePathError {
        FilePathError {
            msg: msg.to_string(),
        }
    }
}

impl From<core::num::ParseIntError> for FilePathError {
    fn from(e: core::num::ParseIntError) -> Self {
        return FilePathError {
            msg: format!("invalid filename: {}", e),
        };
    }
}

impl fmt::Display for FilePathError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid filename")
    }
}

/// Convert Postgres relation file's fork suffix to fork number.
pub fn forkname_to_number(forkname: Option<&str>) -> Result<u8, FilePathError> {
    match forkname {
        // "main" is not in filenames, it's implicit if the fork name is not present
        None => Ok(0),
        Some("fsm") => Ok(1),
        Some("vm") => Ok(2),
        Some("init") => Ok(3),
        Some(_) => Err(FilePathError::new("invalid forkname")),
    }
}

/// Convert Postgres fork number to the right suffix of the relation data file.
pub fn forknumber_to_name(forknum: u8) -> Option<&'static str> {
    match forknum {
        0 => None,
        1 => Some("fsm"),
        2 => Some("vm"),
        3 => Some("init"),
        _ => panic!("unrecognized fork number"),
    }
}

///
/// Parse a filename of a relation file. Returns (relfilenode, forknum, segno) tuple.
///
/// Formats:
/// <oid>
/// <oid>_<fork name>
/// <oid>.<segment number>
/// <oid>_<fork name>.<segment number>
pub fn parse_relfilename(fname: &str) -> Result<(u32, u8, u32), FilePathError> {
    let re = Regex::new(r"^(?P<relnode>\d+)(_(?P<forkname>[a-z]+))?(\.(?P<segno>\d+))?$").unwrap();

    let caps = re
        .captures(fname)
        .ok_or_else(|| FilePathError::new("invalid relation data file name"))?;

    let relnode_str = caps.name("relnode").unwrap().as_str();
    let relnode = u32::from_str_radix(relnode_str, 10)?;

    let forkname = caps.name("forkname").map(|f| f.as_str());
    let forknum = forkname_to_number(forkname)?;

    let segno_match = caps.name("segno");
    let segno = if segno_match.is_none() {
        0
    } else {
        u32::from_str_radix(segno_match.unwrap().as_str(), 10)?
    };

    Ok((relnode, forknum, segno))
}
