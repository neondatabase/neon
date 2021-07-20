use crate::repository::{BufferTag, RelTag};
use crate::waldecoder::TransactionId;
use crate::ZTimelineId;
use serde::{Deserialize, Serialize};

///
/// ObjectKey is the key type used to identify objects stored in an object
/// repository. It is shared between object_repository.rs and object_store.rs.
/// It is mostly opaque to ObjectStore, it just stores and retrieves objects
/// using the key given by the caller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectKey {
    pub timeline: ZTimelineId,
    pub tag: ObjectTag,
}

///
/// Non-relation transaction status files (clog (a.k.a. pg_xact) and pg_multixact)
/// in Postgres are handled by SLRU (Simple LRU) buffer, hence the name.
///
/// These files are global for a postgres instance.
///
/// These files are divided into segments, which are divided into pages
/// of the same BLCKSZ as used for relation files.
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlruBufferTag {
    pub blknum: u32,
}

///
/// Special type of Postgres files: pg_filenode.map is needed to map
/// catalog table OIDs to filenode numbers, which define filename.
///
/// Each database has a map file for its local mapped catalogs,
/// and there is a separate map file for shared catalogs.
///
/// These files have untypical size of 512 bytes.
///
/// See PostgreSQL relmapper.c for details.
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatabaseTag {
    pub spcnode: u32,
    pub dbnode: u32,
}

///
/// Non-relation files that keep state for prepared transactions.
/// Unlike other files these are not divided into pages.
///
/// See PostgreSQL twophase.c for details.
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrepareTag {
    pub xid: TransactionId,
}

/// ObjectTag is a part of ObjectKey that is specific to the type of
/// the stored object.
///
/// NB: the order of the enum values is significant!  In particular,
/// rocksdb_storage.rs assumes that TimelineMetadataTag is first
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectTag {
    // dummy tag preceeding all other keys
    FirstTag,
    TimelineMetadataTag,
    // Special entry that represents PostgreSQL checkpoint.
    // We use it to track fields needed to restore controlfile checkpoint.
    Checkpoint,
    // Various types of non-relation files.
    // We need them to bootstrap compute node.
    ControlFile,
    Clog(SlruBufferTag),
    MultiXactMembers(SlruBufferTag),
    MultiXactOffsets(SlruBufferTag),
    FileNodeMap(DatabaseTag),
    TwoPhase(PrepareTag),
    // put relations at the end of enum to allow efficient iterations through non-rel objects
    RelationMetadata(RelTag),
    RelationBuffer(BufferTag),
}
