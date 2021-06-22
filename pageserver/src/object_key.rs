use crate::repository::{BufferTag, RelTag};
use crate::ZTimelineId;
use serde::{Deserialize, Serialize};

///
/// ObjectKey is the key type used to identify objects stored in an object
/// repository. It is shared between object_repository.rs and object_store.rs.
/// It is mostly opaque to ObjectStore, it just stores and retrieves objects
/// using the key given by the caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectKey {
    pub timeline: ZTimelineId,
    pub tag: ObjectTag,
}

/// ObjectTag is a part of ObjectKey that is specific to the type of
/// the stored object.
///
/// NB: the order of the enum values is significant!  In particular,
/// rocksdb_storage.rs assumes that TimelineMetadataTag is first
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectTag {
    TimelineMetadataTag,
    RelationMetadata(RelTag),
    RelationBuffer(BufferTag),
}
