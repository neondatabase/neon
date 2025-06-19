// Fork numbers, from relpath.h
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
