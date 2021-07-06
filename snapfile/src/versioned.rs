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
#[allow(dead_code)] // FIXME: this is a placeholder for future functionality.
pub(crate) const CHAPTER_SNAP_META: u64 = 1;
/// A packed set of 8KB pages.
pub(crate) const CHAPTER_PAGES: u64 = 2;
/// An index of pages.
pub(crate) const CHAPTER_PAGE_INDEX: u64 = 3;

// FIXME: move serialized data structs to a separate file.

/// An index from page number to offset within the pages chapter.
#[derive(Debug, Default, Serialize, Deserialize, Versioned, UpgradeLatest)]
pub struct PageIndexV1 {
    /// A map from page number to file offset.
    pub(crate) map: BTreeMap<u64, u64>,
}

// A placeholder type, that will always point to the latest version.
pub type PageIndex = PageIndexV1;

// Each message gets a unique message id, for tracking by the aversion traits.
assign_message_ids! {
    PageIndex: 100,
}
