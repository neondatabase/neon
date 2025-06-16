use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Deserialize_repr, Serialize_repr)]
#[repr(u32)]
pub enum PgMajorVersion {
    PG14 = 14,
    PG15 = 15,
    PG16 = 16,
    PG17 = 17,
}

pub type PgVersionId = u32;

impl PgMajorVersion {
    pub const fn major_version_num(&self) -> u32 {
        match self {
            PgMajorVersion::PG14 => 14,
            PgMajorVersion::PG15 => 15,
            PgMajorVersion::PG16 => 16,
            PgMajorVersion::PG17 => 17,
        }
    }

    pub fn versionfile_string(&self) -> String {
        match self {
            PgMajorVersion::PG17 => "17\x0A".to_string(),
            PgMajorVersion::PG16 => "16\x0A".to_string(),
            PgMajorVersion::PG15 => "15".to_string(),
            PgMajorVersion::PG14 => "14".to_string(),
        }
    }

    pub fn v_str(&self) -> String {
        match self {
            PgMajorVersion::PG17 => "v17".to_string(),
            PgMajorVersion::PG16 => "v16".to_string(),
            PgMajorVersion::PG15 => "v15".to_string(),
            PgMajorVersion::PG14 => "v14".to_string(),
        }
    }

    pub const fn all() -> [PgMajorVersion; 4] {
        [
            PgMajorVersion::PG14,
            PgMajorVersion::PG15,
            PgMajorVersion::PG16,
            PgMajorVersion::PG17,
        ]
    }
}

impl Display for PgMajorVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PgMajorVersion::PG14 => {
                write!(f, "PgMajorVersion::PG14")
            }
            PgMajorVersion::PG15 => {
                write!(f, "PgMajorVersion::PG15")
            }
            PgMajorVersion::PG16 => {
                write!(f, "PgMajorVersion::PG16")
            }
            PgMajorVersion::PG17 => {
                write!(f, "PgMajorVersion::PG17")
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct InvalidPgVersion(u32);

impl TryFrom<PgVersionId> for PgMajorVersion {
    type Error = InvalidPgVersion;

    fn try_from(value: PgVersionId) -> Result<Self, Self::Error> {
        Ok(match value / 10000 {
            14 => PgMajorVersion::PG14,
            15 => PgMajorVersion::PG15,
            16 => PgMajorVersion::PG16,
            17 => PgMajorVersion::PG17,
            _ => {
                panic!("invalid pg version ID {value}");
            }
        })
    }
}

impl From<PgMajorVersion> for PgVersionId {
    fn from(value: PgMajorVersion) -> Self {
        ((value as u32) * 10000) as PgVersionId
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("PgMajorVersionParseError")]
pub struct PgMajorVersionParseError(String);

impl FromStr for PgMajorVersion {
    type Err = PgMajorVersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "14" => Ok(PgMajorVersion::PG14),
            "15" => Ok(PgMajorVersion::PG15),
            "16" => Ok(PgMajorVersion::PG16),
            "17" => Ok(PgMajorVersion::PG17),
            _ => Err(PgMajorVersionParseError(s.to_string())),
        }
    }
}
