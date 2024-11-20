use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

use postgres_ffi::pg_constants::GLOBALTABLESPACE_OID;
use postgres_ffi::relfile_utils::{forkname_to_number, forknumber_to_name, MAIN_FORKNUM};
use postgres_ffi::Oid;

///
/// Relation data file segment id throughout the Postgres cluster.
///
/// Every data file in Postgres is uniquely identified by 4 numbers:
/// - relation id / node (`relnode`)
/// - database id (`dbnode`)
/// - tablespace id (`spcnode`), in short this is a unique id of a separate
///   directory to store data files.
/// - forknumber (`forknum`) is used to split different kinds of data of the same relation
///   between some set of files (`relnode`, `relnode_fsm`, `relnode_vm`).
///
/// In native Postgres code `RelFileNode` structure and individual `ForkNumber` value
/// are used for the same purpose.
/// [See more related comments here](https:///github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/relfilenode.h#L57).
///
// FIXME: should move 'forknum' as last field to keep this consistent with Postgres.
// Then we could replace the custom Ord and PartialOrd implementations below with
// deriving them. This will require changes in walredoproc.c.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct RelTag {
    pub forknum: u8,
    pub spcnode: Oid,
    pub dbnode: Oid,
    pub relnode: Oid,
}

/// Block number within a relation or SLRU. This matches PostgreSQL's BlockNumber type.
pub type BlockNumber = u32;

impl PartialOrd for RelTag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelTag {
    fn cmp(&self, other: &Self) -> Ordering {
        // Custom ordering where we put forknum to the end of the list
        let other_tup = (other.spcnode, other.dbnode, other.relnode, other.forknum);
        (self.spcnode, self.dbnode, self.relnode, self.forknum).cmp(&other_tup)
    }
}

/// Display RelTag in the same format that's used in most PostgreSQL debug messages:
///
/// ```text
/// <spcnode>/<dbnode>/<relnode>[_fsm|_vm|_init]
/// ```
impl fmt::Display for RelTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(forkname) = forknumber_to_name(self.forknum) {
            write!(
                f,
                "{}/{}/{}_{}",
                self.spcnode, self.dbnode, self.relnode, forkname
            )
        } else {
            write!(f, "{}/{}/{}", self.spcnode, self.dbnode, self.relnode)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseRelTagError {
    #[error("invalid forknum")]
    InvalidForknum(#[source] std::num::ParseIntError),
    #[error("missing triplet member {}", .0)]
    MissingTripletMember(usize),
    #[error("invalid triplet member {}", .0)]
    InvalidTripletMember(usize, #[source] std::num::ParseIntError),
}

impl std::str::FromStr for RelTag {
    type Err = ParseRelTagError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ParseRelTagError::*;

        // FIXME: in postgres logs this separator is dot
        // Example:
        //     could not read block 2 in rel 1663/208101/2620.1 from page server at lsn 0/2431E6F0
        // with a regex we could get this more painlessly
        let (triplet, forknum) = match s.split_once('_').or_else(|| s.split_once('.')) {
            Some((t, f)) => {
                let forknum = forkname_to_number(Some(f));
                let forknum = if let Ok(f) = forknum {
                    f
                } else {
                    f.parse::<u8>().map_err(InvalidForknum)?
                };

                (t, Some(forknum))
            }
            None => (s, None),
        };

        let mut split = triplet
            .splitn(3, '/')
            .enumerate()
            .map(|(i, s)| s.parse::<u32>().map_err(|e| InvalidTripletMember(i, e)));
        let spcnode = split.next().ok_or(MissingTripletMember(0))??;
        let dbnode = split.next().ok_or(MissingTripletMember(1))??;
        let relnode = split.next().ok_or(MissingTripletMember(2))??;

        Ok(RelTag {
            spcnode,
            forknum: forknum.unwrap_or(MAIN_FORKNUM),
            dbnode,
            relnode,
        })
    }
}

impl RelTag {
    pub fn to_segfile_name(&self, segno: u32) -> String {
        let mut name = if self.spcnode == GLOBALTABLESPACE_OID {
            "global/".to_string()
        } else {
            format!("base/{}/", self.dbnode)
        };

        name += &self.relnode.to_string();

        if let Some(fork_name) = forknumber_to_name(self.forknum) {
            name += "_";
            name += fork_name;
        }

        if segno != 0 {
            name += ".";
            name += &segno.to_string();
        }

        name
    }

    pub fn with_forknum(&self, forknum: u8) -> Self {
        RelTag {
            forknum,
            spcnode: self.spcnode,
            dbnode: self.dbnode,
            relnode: self.relnode,
        }
    }
}

///
/// Non-relation transaction status files (clog (a.k.a. pg_xact) and
/// pg_multixact) in Postgres are handled by SLRU (Simple LRU) buffer,
/// hence the name.
///
/// These files are global for a postgres instance.
///
/// These files are divided into segments, which are divided into
/// pages of the same BLCKSZ as used for relation files.
///
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum_macros::EnumIter,
    strum_macros::FromRepr,
    enum_map::Enum,
)]
#[repr(u8)]
pub enum SlruKind {
    Clog = 0,
    MultiXactMembers,
    MultiXactOffsets,
}

impl SlruKind {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Clog => "pg_xact",
            Self::MultiXactMembers => "pg_multixact/members",
            Self::MultiXactOffsets => "pg_multixact/offsets",
        }
    }
}
