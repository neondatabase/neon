use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

use postgres_ffi::pg_constants::GLOBALTABLESPACE_OID;
use postgres_ffi::relfile_utils::forknumber_to_name;
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
// Then we could replace the custo Ord and PartialOrd implementations below with
// deriving them.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct RelTag {
    pub forknum: u8,
    pub spcnode: Oid,
    pub dbnode: Oid,
    pub relnode: Oid,
}

impl PartialOrd for RelTag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelTag {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut cmp = self.spcnode.cmp(&other.spcnode);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.dbnode.cmp(&other.dbnode);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.relnode.cmp(&other.relnode);
        if cmp != Ordering::Equal {
            return cmp;
        }
        cmp = self.forknum.cmp(&other.forknum);

        cmp
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
#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlruKind {
    Clog,
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
