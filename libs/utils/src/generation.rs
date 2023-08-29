use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Generation {
    // Generations with this magic value will not add a suffix to S3 keys, and will not
    // be included in persisted index_part.json.  This value is only to be used
    // during migration from pre-generation metadata to generation-aware metadata,
    // and should eventually go away.
    //
    // A special Generation is used rather than always wrapping Generation in an Option,
    // so that code handling generations doesn't have to be aware of the legacy
    // case everywhere it touches a generation.
    None,
    // Generations with this magic value may never be used to construct S3 keys:
    // we will panic if someone tries to.  This is for Tenants in the "Broken" state,
    // so that we can satisfy their constructor with a Generation without risking
    // a code bug using it in an S3 write (broken tenants should never write)
    Broken,
    Valid(u32),
}

/// The Generation type represents a number associated with a Tenant, which
/// increments every time the tenant is attached to a new pageserver, or
/// an attached pageserver restarts.
///
/// It is included as a suffix in S3 keys, as a protection against split-brain
/// scenarios where pageservers might otherwise issue conflicting writes to
/// remote storage
impl Generation {
    /// Create a new Generation that represents a legacy key format with
    /// no generation suffix
    pub fn none() -> Self {
        Self::None
    }

    // Create a new generation that will panic if you try to use get_suffix
    pub fn broken() -> Self {
        Self::Broken
    }

    pub fn new(v: u32) -> Self {
        Self::Valid(v)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub fn get_suffix(&self) -> String {
        match self {
            Self::Valid(v) => {
                format!("-{:08x}", v)
            }
            Self::None => "".into(),
            Self::Broken => {
                panic!("Tried to use a broken generation");
            }
        }
    }
}

impl Serialize for Generation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Self::Valid(v) = self {
            v.serialize(serializer)
        } else {
            // We should never be asked to serialize a None or Broken.  Structures
            // that include an optional generation should convert None to an
            // Option<Generation>::None
            Err(serde::ser::Error::custom(
                "Tried to serialize invalid generation",
            ))
        }
    }
}

impl<'de> Deserialize<'de> for Generation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::Valid(u32::deserialize(deserializer)?))
    }
}

impl Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Valid(v) => {
                write!(f, "{:08x}", v)
            }
            Self::None => {
                write!(f, "<none>")
            }
            Self::Broken => {
                write!(f, "<broken>")
            }
        }
    }
}
