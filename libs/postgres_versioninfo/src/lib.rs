use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// An enum with one variant for each major version of PostgreSQL that we support.
///
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Deserialize_repr, Serialize_repr)]
#[repr(u32)]
pub enum PgMajorVersion {
    PG14 = 14,
    PG15 = 15,
    PG16 = 16,
    PG17 = 17,
    // !!! When you add a new PgMajorVersion, don't forget to update PgMajorVersion::ALL
}

/// A full PostgreSQL version ID, in MMmmbb numerical format (Major/minor/bugfix)
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[repr(transparent)]
pub struct PgVersionId(u32);

impl PgVersionId {
    pub const UNKNOWN: PgVersionId = PgVersionId(0);

    pub fn from_full_pg_version(version: u32) -> PgVersionId {
        match version {
            0 => PgVersionId(version), // unknown version
            140000..180000 => PgVersionId(version),
            _ => panic!("Invalid full PostgreSQL version ID {version}"),
        }
    }
}

impl Display for PgVersionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        u32::fmt(&self.0, f)
    }
}

impl Serialize for PgVersionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        u32::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for PgVersionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize(deserializer).map(PgVersionId)
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize_in_place(deserializer, &mut place.0)
    }
}

impl PgMajorVersion {
    /// Get the numerical representation of the represented Major Version
    pub const fn major_version_num(&self) -> u32 {
        match self {
            PgMajorVersion::PG14 => 14,
            PgMajorVersion::PG15 => 15,
            PgMajorVersion::PG16 => 16,
            PgMajorVersion::PG17 => 17,
        }
    }

    /// Get the contents of this version's PG_VERSION file.
    ///
    /// The PG_VERSION file is used to determine the PostgreSQL version that currently
    /// owns the data in a PostgreSQL data directory.
    pub fn versionfile_string(&self) -> &'static str {
        match self {
            PgMajorVersion::PG14 => "14",
            PgMajorVersion::PG15 => "15",
            PgMajorVersion::PG16 => "16\x0A",
            PgMajorVersion::PG17 => "17\x0A",
        }
    }

    /// Get the v{version} string of this major PostgreSQL version.
    ///
    /// Because this was hand-coded in various places, this was moved into a shared
    /// implementation.
    pub fn v_str(&self) -> String {
        match self {
            PgMajorVersion::PG14 => "v14",
            PgMajorVersion::PG15 => "v15",
            PgMajorVersion::PG16 => "v16",
            PgMajorVersion::PG17 => "v17",
        }
        .to_string()
    }

    /// All currently supported major versions of PostgreSQL.
    pub const ALL: &'static [PgMajorVersion] = &[
        PgMajorVersion::PG14,
        PgMajorVersion::PG15,
        PgMajorVersion::PG16,
        PgMajorVersion::PG17,
    ];
}

impl Display for PgMajorVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PgMajorVersion::PG14 => "PgMajorVersion::PG14",
            PgMajorVersion::PG15 => "PgMajorVersion::PG15",
            PgMajorVersion::PG16 => "PgMajorVersion::PG16",
            PgMajorVersion::PG17 => "PgMajorVersion::PG17",
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub struct InvalidPgVersion(u32);

impl Display for InvalidPgVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InvalidPgVersion({})", self.0)
    }
}

impl TryFrom<PgVersionId> for PgMajorVersion {
    type Error = InvalidPgVersion;

    fn try_from(value: PgVersionId) -> Result<Self, Self::Error> {
        Ok(match value.0 / 10000 {
            14 => PgMajorVersion::PG14,
            15 => PgMajorVersion::PG15,
            16 => PgMajorVersion::PG16,
            17 => PgMajorVersion::PG17,
            _ => return Err(InvalidPgVersion(value.0)),
        })
    }
}

impl From<PgMajorVersion> for PgVersionId {
    fn from(value: PgMajorVersion) -> Self {
        PgVersionId((value as u32) * 10000)
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub struct PgMajorVersionParseError(String);

impl Display for PgMajorVersionParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PgMajorVersionParseError({})", self.0)
    }
}

impl FromStr for PgMajorVersion {
    type Err = PgMajorVersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "14" => PgMajorVersion::PG14,
            "15" => PgMajorVersion::PG15,
            "16" => PgMajorVersion::PG16,
            "17" => PgMajorVersion::PG17,
            _ => return Err(PgMajorVersionParseError(s.to_string())),
        })
    }
}
