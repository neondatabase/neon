//!
//! FIXME: relishes are obsolete
//!
//! Zenith stores PostgreSQL relations, and some other files, in the
//! repository.  The relations (i.e. tables and indexes) take up most
//! of the space in a typical installation, while the other files are
//! small. We call each relation and other file that is stored in the
//! repository a "relish". It comes from "rel"-ish, as in "kind of a
//! rel", because it covers relations as well as other things that are
//! not relations, but are treated similarly for the purposes of the
//! storage layer.
//!
//! This source file contains the definition of the RelishTag struct,
//! which uniquely identifies a relish.
//!
//! Relishes come in two flavors: blocky and non-blocky. Relations and
//! SLRUs are blocky, that is, they are divided into 8k blocks, and
//! the repository tracks their size. Other relishes are non-blocky:
//! the content of the whole relish is stored as one blob. Block
//! number must be passed as 0 for all operations on a non-blocky
//! relish. The one "block" that you store in a non-blocky relish can
//! have arbitrary size, but they are expected to be small, or you
//! will have performance issues.
//!
//! All relishes are versioned by LSN in the repository.
//!

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

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
        let mut cmp;

        cmp = self.spcnode.cmp(&other.spcnode);
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
/// <spcnode>/<dbnode>/<relnode>[_fsm|_vm|_init]
///
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
