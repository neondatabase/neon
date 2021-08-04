//!
//! Common structs shared by object_repository.rs and object_store.rs.
//!

use crate::relish::RelishTag;
use serde::{Deserialize, Serialize};
use zenith_utils::zid::ZTimelineId;

///
/// ObjectKey is the key type used to identify objects stored in an object
/// repository. It is shared between object_repository.rs and object_store.rs.
/// It is mostly opaque to ObjectStore, it just stores and retrieves objects
/// using the key given by the caller.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectKey {
    pub timeline: ZTimelineId,
    pub tag: ObjectTag,
}

///
/// ObjectTag is a part of ObjectKey that is specific to the type of
/// the stored object.
///
/// NB: the order of the enum values is significant!  In particular,
/// rocksdb_storage.rs assumes that TimelineMetadataTag is first
///
/// Buffer is the kind of object that is accessible by the public
/// get_page_at_lsn() / put_page_image() / put_wal_record() functions in
/// the repository.rs interface. The rest are internal objects stored in
/// the key-value store, to store various metadata. They're not directly
/// accessible outside object_repository.rs
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectTag {
    // dummy tag preceeding all other keys
    FirstTag,

    // Metadata about a timeline. Not versioned.
    TimelineMetadataTag,

    // These objects store metadata about one relish. Currently it's used
    // just to track the relish's size. It's not used for non-blocky relishes
    // at all.
    RelationMetadata(RelishTag),

    // These are the pages exposed in the public Repository/Timeline interface.
    Buffer(RelishTag, u32),
}
