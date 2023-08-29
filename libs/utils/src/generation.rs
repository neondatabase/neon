use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct Generation(u32);

/// The Generation type represents a number associated with a Tenant, which
/// increments every time the tenant is attached to a new pageserver, or
/// an attached pageserver restarts.
///
/// It is included as a suffix in S3 keys, as a protection against split-brain
/// scenarios where pageservers might otherwise issue conflicting writes to
/// remote storage
impl Generation {
    // Generations with this magic value may never be used to construct S3 keys:
    // we will panic if someone tries to.  This is for Tenants in the "Broken" state,
    // so that we can satisfy their constructor with a Generation without risking
    // a code bug using it in an S3 write (broken tenants should never write)
    const BROKEN: u32 = u32::MAX;

    // Generations with this magic value will not add a suffix to S3 keys, and will not
    // be included in persisted index_part.json.  This value is only to be used
    // during migration from pre-generation metadata to generation-aware metadata,
    // and should eventually go away.
    //
    // A special Generation is used rather than always wrapping Generation in an Option,
    // so that code handling generations doesn't have to be aware of the legacy
    // case everywhere it touches a generation.
    const NONE: u32 = u32::MAX - 1;

    /// Create a new Generation that represents a legacy key format with
    /// no generation suffix
    pub fn none() -> Self {
        Self(Self::NONE)
    }

    // Create a new generation that will panic if you try to use get_suffix
    pub fn broken() -> Self {
        Self(Self::BROKEN)
    }

    pub fn new(v: u32) -> Self {
        assert!(v != Self::BROKEN);

        Self(v)
    }

    pub fn is_none(&self) -> bool {
        self.0 == Self::NONE
    }

    pub fn get_suffix(&self) -> String {
        assert!(self.0 != Self::BROKEN, "Tried to use a broken generation");
        if self.0 == Self::NONE {
            "".into()
        } else {
            format!("-{:08x}", self.0).into()
        }
    }
}

impl Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == Self::BROKEN {
            write!(f, "<broken>")
        } else if self.0 == Self::NONE {
            write!(f, "<none>")
        } else {
            write!(f, "{:08x}", self.0)
        }
    }
}
