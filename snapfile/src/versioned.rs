//! Versioned data structures for snapshot files
//!
//! To ensure that future versions of software can read snapshot files,
//! all data structures that are serialized into the snapshot files should
//! live in this module.
//!
//! Once released, versioned data structures should never be modified.
//! Instead, new versions should be created and conversion functions should
//! be defined using the `FromVersion` trait.

use aversion::{assign_message_ids, UpgradeLatest, Versioned};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// A random constant, to identify this file type.
pub(crate) const SNAPFILE_MAGIC: u32 = 0x7fb8_38a8;

// Constant chapter numbers
// FIXME: the bookfile crate should use something better to index, e.g. strings.
/// Snapshot-specific file metadata
pub(crate) const CHAPTER_SNAP_META: u64 = 1;
/// A packed set of 8KB pages.
pub(crate) const CHAPTER_PAGES: u64 = 2;
/// An index of pages.
pub(crate) const CHAPTER_PAGE_INDEX: u64 = 3;

/// Information about the predecessor snapshot.
///
/// It contains the snap_id of the predecessor snapshot, and the LSN
/// of that snapshot.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Predecessor {
    /// This is the ID number of the predecessor timeline.
    ///
    /// This may match the current snapshot's timeline id, but
    /// it may not (if the precessor was the branch point).
    pub timeline: [u8; 16],

    /// This is the LSN of the predecessor snapshot.
    pub lsn: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Versioned, UpgradeLatest)]
pub struct SnapFileMetaV1 {
    /// This is a unique ID number for this timeline.
    ///
    /// This number guarantees that snapshot history is unique.
    pub timeline: [u8; 16],

    /// Information about the predecessor snapshot.
    ///
    /// If `None`, this snapshot is the start of a new database.
    pub predecessor: Option<Predecessor>,

    /// This is the last LSN stored in this snapshot.
    pub lsn: u64,
}

/// A type alias for the latest version of `SnapFileMeta`.
pub type SnapFileMeta = SnapFileMetaV1;

/// A page location within a file.
///
/// Note: this is an opaque value that may not be the true byte offset;
/// it may be relative to some other location or measured in units other
/// than bytes.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PageLocationV1(pub(crate) u64);

/// A type alias for the latest version of `PageLocation`.
pub type PageLocation = PageLocationV1;

/// An index from page number to offset within the pages chapter.
#[derive(Debug, Default, Serialize, Deserialize, Versioned, UpgradeLatest)]
pub struct PageIndexV1 {
    /// A map from page number to file offset.
    pub(crate) map: BTreeMap<u64, PageLocationV1>,
}

/// A type alias for the latest version of `PageIndex`.
pub type PageIndex = PageIndexV1;

// Each message gets a unique message id, for tracking by the aversion traits.
assign_message_ids! {
    PageIndex: 100,
    SnapFileMeta: 101,
}
