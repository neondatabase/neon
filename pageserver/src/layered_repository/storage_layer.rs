//!
//! Common traits and structs for layers
//!

use crate::relish::RelishTag;
use crate::repository::WALRecord;
use crate::ZTimelineId;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

use zenith_utils::lsn::Lsn;

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

impl PageVersion {
    pub fn get_mem_size(&self) -> usize {
        let mut sz = 0;

        // every page version has some fixed overhead.
        sz += 16;

        if let Some(img) = &self.page_image {
            sz += img.len();
        }

        if let Some(rec) = &self.record {
            sz += rec.rec.len();

            // Some per-record overhead. Not very accurate, but close enough
            sz += 32;
        }

        sz
    }
}

///
/// Data needed to reconstruct a page version
///
/// 'page_img' is the old base image of the page to start the WAL replay with.
/// It can be None, if the first WAL record initializes the page (will_init)
/// 'records' contains the records to apply over the base image.
///
pub struct PageReconstructData {
    pub records: Vec<WALRecord>,
    pub page_img: Option<Bytes>,
}

///
/// A Layer holds all page versions for one segment of a relish, in a range of LSNs.
/// There are two kinds of layers, in-memory and snapshot layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access
/// to the recent page versions. Snaphot layers are stored on disk, and
/// are immutable. This trait presents the common functionality of
/// in-memory and snapshot layers.
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

    ///
    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// If returns Some, the returned data is not complete. The caller needs
    /// to continue with the returned 'lsn'.
    ///
    /// Note that the 'blknum' is the offset of the page from the beginning
    /// of the *relish*, not the beginning of the segment. The requested
    /// 'blknum' must be covered by this segment.
    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<Option<Lsn>>;

    // Functions that correspond to the Timeline trait functions.
    fn get_seg_size(&self, lsn: Lsn) -> Result<u32>;

    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool>;
}
