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
use std::fmt;

use postgres_ffi::relfile_utils::forknumber_to_name;
use postgres_ffi::{Oid, TransactionId};

///
/// RelishTag identifies one relish.
///
#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelishTag {
    // Relations correspond to PostgreSQL relation forks. Each
    // PostgreSQL relation fork is considered a separate relish.
    Relation(RelTag),

    // SLRUs include pg_clog, pg_multixact/members, and
    // pg_multixact/offsets. There are other SLRUs in PostgreSQL, but
    // they don't need to be stored permanently (e.g. pg_subtrans),
    // or we do not support them in zenith yet (pg_commit_ts).
    //
    // These are currently never requested directly by the compute
    // nodes, although in principle that would be possible. However,
    // when a new compute node is created, these are included in the
    // tarball that we send to the compute node to initialize the
    // PostgreSQL data directory.
    //
    // Each SLRU segment in PostgreSQL is considered a separate
    // relish. For example, pg_clog/0000, pg_clog/0001, and so forth.
    //
    // SLRU segments are divided into blocks, like relations.
    Slru { slru: SlruKind, segno: u32 },

    // Miscellaneous other files that need to be included in the
    // tarball at compute node creation. These are non-blocky, and are
    // expected to be small.

    //
    // FileNodeMap represents PostgreSQL's 'pg_filenode.map'
    // files. They are needed to map catalog table OIDs to filenode
    // numbers. Usually the mapping is done by looking up a relation's
    // 'relfilenode' field in the 'pg_class' system table, but that
    // doesn't work for 'pg_class' itself and a few other such system
    // relations. See PostgreSQL relmapper.c for details.
    //
    // Each database has a map file for its local mapped catalogs,
    // and there is a separate map file for shared catalogs.
    //
    // These files are always 512 bytes long (although we don't check
    // or care about that in the page server).
    //
    FileNodeMap { spcnode: Oid, dbnode: Oid },

    //
    // State files for prepared transactions (e.g pg_twophase/1234)
    //
    TwoPhase { xid: TransactionId },

    // The control file, stored in global/pg_control
    ControlFile,

    // Special entry that represents PostgreSQL checkpoint. It doesn't
    // correspond to to any physical file in PostgreSQL, but we use it
    // to track fields needed to restore the checkpoint data in the
    // control file, when a compute node is created.
    Checkpoint,
}

impl RelishTag {
    pub const fn is_blocky(&self) -> bool {
        match self {
            // These relishes work with blocks
            RelishTag::Relation(_) | RelishTag::Slru { slru: _, segno: _ } => true,

            // and these don't
            RelishTag::FileNodeMap {
                spcnode: _,
                dbnode: _,
            }
            | RelishTag::TwoPhase { xid: _ }
            | RelishTag::ControlFile
            | RelishTag::Checkpoint => false,
        }
    }

    // Physical relishes represent files and use
    // RelationSizeEntry to track existing and dropped files.
    // They can be both blocky and non-blocky.
    pub const fn is_physical(&self) -> bool {
        match self {
            // These relishes represent physical files
            RelishTag::Relation(_)
            | RelishTag::Slru { .. }
            | RelishTag::FileNodeMap { .. }
            | RelishTag::TwoPhase { .. } => true,

            // and these don't
            | RelishTag::ControlFile
            | RelishTag::Checkpoint => false,
        }
    }
}

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
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct RelTag {
    pub forknum: u8,
    pub spcnode: Oid,
    pub dbnode: Oid,
    pub relnode: Oid,
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

/// Display RelTag in the same format that's used in most PostgreSQL debug messages:
///
/// <spcnode>/<dbnode>/<relnode>[_fsm|_vm|_init]
///
impl fmt::Display for RelishTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelishTag::Relation(rel) => rel.fmt(f),
            RelishTag::Slru { slru, segno } => {
                // e.g. pg_clog/0001
                write!(f, "{}/{:04X}", slru.to_str(), segno)
            }
            RelishTag::FileNodeMap { spcnode, dbnode } => {
                write!(f, "relmapper file for spc {} db {}", spcnode, dbnode)
            }
            RelishTag::TwoPhase { xid } => {
                write!(f, "pg_twophase/{:08X}", xid)
            }
            RelishTag::ControlFile => {
                write!(f, "control file")
            }
            RelishTag::Checkpoint => {
                write!(f, "checkpoint")
            }
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

pub const FIRST_NONREL_RELISH_TAG: RelishTag = RelishTag::Slru {
    slru: SlruKind::Clog,
    segno: 0,
};
