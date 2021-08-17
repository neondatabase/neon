//!
//! Common traits and structs for layers
//!

use crate::relish::RelishTag;
use crate::repository::WALRecord;
use crate::walredo::WalRedoManager;
use crate::ZTimelineId;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use zenith_utils::lsn::Lsn;

pub static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// Size of one segment in pages (10 MB)
pub const RELISH_SEG_SIZE: u32 = 10 * 1024 * 1024 / 8192;

///
/// Each relish stored in the repository is divided into fixed-sized "segments",
/// with 10 MB of key-space, or 1280 8k pages each.
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Clone, Copy)]
pub struct SegmentTag {
    pub rel: RelishTag,
    pub segno: u32,
}

impl fmt::Display for SegmentTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.rel, self.segno)
    }
}

impl SegmentTag {
    pub const fn from_blknum(rel: RelishTag, blknum: u32) -> SegmentTag {
        SegmentTag {
            rel,
            segno: blknum / RELISH_SEG_SIZE,
        }
    }

    pub fn blknum_in_seg(&self, blknum: u32) -> bool {
        blknum / RELISH_SEG_SIZE == self.segno
    }
}

///
/// Represents a version of a page at a specific LSN. The LSN is the key of the
/// entry in the 'page_versions' hash, it is not duplicated here.
///
/// A page version can be stored as a full page image, or as WAL record that needs
/// to be applied over the previous page version to reconstruct this version.
///
/// It's also possible to have both a WAL record and a page image in the same
/// PageVersion. That happens if page version is originally stored as a WAL record
/// but it is later reconstructed by a GetPage@LSN request by performing WAL
/// redo. The get_page_at_lsn() code will store the reconstructed pag image next to
/// the WAL record in that case. TODO: That's pretty accidental, not the result
/// of any grand design. If we want to keep reconstructed page versions around, we
/// probably should have a separate buffer cache so that we could control the
/// replacement policy globally. Or if we keep a reconstructed page image, we
/// could throw away the WAL record.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageVersion {
    /// an 8kb page image
    pub page_image: Option<Bytes>,
    /// WAL record to get from previous page version to this one.
    pub record: Option<WALRecord>,
}

///
/// A Layer holds all page versions for one segment of a relish, in a range of LSNs.
/// There are two kinds of layers, in-memory and snapshot layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access
/// to the recent page versions. Snaphot layers are stored on disk, and
/// are immutable.
///
/// Each layer contains a full snapshot of the segment at the start
/// LSN. In addition to that, it contains WAL (or more page images)
/// needed to recontruct any page version up to the end LSN.
///
pub trait Layer: Send + Sync {
    // These functions identify the relish segment and the LSN range
    // that this Layer holds.
    fn get_timeline_id(&self) -> ZTimelineId;
    fn get_seg_tag(&self) -> SegmentTag;
    fn get_start_lsn(&self) -> Lsn;
    fn get_end_lsn(&self) -> Lsn;
    fn is_dropped(&self) -> bool;

    /// Frozen layers are stored on disk, an cannot accept cannot accept new WAL
    /// records, whereas an unfrozen layer can still be modified, but is not
    /// durable in case of a crash. Snapshot layers are always frozen, and
    /// in-memory layers are always unfrozen.
    fn is_frozen(&self) -> bool;

    // Functions that correspond to the Timeline trait functions.

    // Note that the 'blknum' is the offset of the page from the beginning
    // of the *relish*, not the beginning of the segment. The requested
    // 'blknum' must be covered by this segment.
    fn get_page_at_lsn(
        &self,
        walredo_mgr: &dyn WalRedoManager,
        blknum: u32,
        lsn: Lsn,
    ) -> Result<Bytes>;

    fn get_seg_size(&self, lsn: Lsn) -> Result<u32>;

    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool>;

    fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> Result<()>;

    fn put_truncation(&self, lsn: Lsn, relsize: u32) -> anyhow::Result<()>;

    fn put_unlink(&self, lsn: Lsn) -> anyhow::Result<()>;

    /// Remember new page version, as a WAL record over previous version
    fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> Result<()> {
        self.put_page_version(
            blknum,
            rec.lsn,
            PageVersion {
                page_image: None,
                record: Some(rec),
            },
        )
    }

    /// Remember new page version, as a full page image
    fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                page_image: Some(img),
                record: None,
            },
        )
    }

    ///
    /// Split off an immutable layer from existing layer.
    ///
    /// Returns new layers that replace this one.
    ///
    fn freeze(&self, end_lsn: Lsn, walredo_mgr: &dyn WalRedoManager)
        -> Result<Vec<Arc<dyn Layer>>>;

    /// Permanently delete this layer
    fn delete(&self) -> Result<()>;

    /// Try to release memory used by this layer. This is currently
    /// only used by snapshot layers, to free the copy of the file
    /// from memory. (TODO: a smarter, more granular caching scheme
    /// would be nice)
    fn unload(&self) -> Result<()>;
}
