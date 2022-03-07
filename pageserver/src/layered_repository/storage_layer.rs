//!
//! Common traits and structs for layers
//!

use crate::repository::{Key, Value};
use crate::walrecord::ZenithWalRecord;
use crate::{ZTenantId, ZTimelineId};
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashSet;
use std::ops::Range;
use std::path::PathBuf;

use zenith_utils::lsn::Lsn;

// in # of key-value pairs
// FIXME Size of one segment in pages (128 MB)
pub const TARGET_FILE_SIZE_BYTES: u64 = 128 * 1024 * 1024;
pub const TARGET_FILE_SIZE: u32 = (TARGET_FILE_SIZE_BYTES / 8192) as u32;

pub fn range_overlaps<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialOrd<T>,
{
    if a.start < b.start {
        a.end > b.start
    } else {
        b.end > a.start
    }
}

/// FIXME
/// Struct used to communicate across calls to 'get_page_reconstruct_data'.
///
/// Before first call to get_page_reconstruct_data, you can fill in 'page_img'
/// if you have an older cached version of the page available. That can save
/// work in 'get_page_reconstruct_data', as it can stop searching for page
/// versions when all the WAL records going back to the cached image have been
/// collected.
///
/// When get_page_reconstruct_data returns Complete, 'page_img' is set to an
/// image of the page, or the oldest WAL record in 'records' is a will_init-type
/// record that initializes the page without requiring a previous image.
///
/// If 'get_page_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same PageReconstructData struct in the next 'get_page_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug)]
pub struct ValueReconstructState {
    pub key: Key,
    pub lsn: Lsn,
    pub records: Vec<(Lsn, ZenithWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,

    pub request_lsn: Lsn, // original request's LSN, for debugging purposes
}

/// Return value from Layer::get_page_reconstruct_data
#[derive(Debug)]
pub enum ValueReconstructResult {
    /// Got all the data needed to reconstruct the requested page
    Complete,
    /// This layer didn't contain all the required data, the caller should look up
    /// the predecessor layer at the returned LSN and collect more data from there.
    Continue,

    /// This layer didn't contain data needed to reconstruct the page version at
    /// the returned LSN. This is usually considered an error, but might be OK
    /// in some circumstances.
    Missing,
}

/// FIXME
/// A Layer corresponds to one RELISH_SEG_SIZE slice of a relish in a range of LSNs.
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access
/// to the recent page versions. On-disk layers are stored as files on disk, and
/// are immutable. This trait presents the common functionality of
/// in-memory and on-disk layers.
///
pub trait Layer: Send + Sync {
    fn get_tenant_id(&self) -> ZTenantId;

    /// Identify the timeline this relish belongs to
    fn get_timeline_id(&self) -> ZTimelineId;

    /// Range of segments that this layer covers
    fn get_key_range(&self) -> Range<Key>;

    /// FIXME
    /// Inclusive start bound of the LSN range that this layer holds
    /// Exclusive end bound of the LSN range that this layer holds.
    ///
    /// - For an open in-memory layer, this is MAX_LSN.
    /// - For a frozen in-memory layer or a delta layer, this is a valid end bound.
    /// - An image layer represents snapshot at one LSN, so end_lsn is always the snapshot LSN + 1
    fn get_lsn_range(&self) -> Range<Lsn>;

    /// Filename used to store this layer on disk. (Even in-memory layers
    /// implement this, to print a handy unique identifier for the layer for
    /// log messages, even though they're never not on disk.)
    fn filename(&self) -> PathBuf;

    ///
    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// See PageReconstructResult for possible return values. The collected data
    /// is appended to reconstruct_data; the caller should pass an empty struct
    /// on first call, or a struct with a cached older image of the page if one
    /// is available. If this returns PageReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data' to
    /// collect more data.
    fn get_value_reconstruct_data(
        &self,
        lsn_floor: Lsn,
        reconstruct_data: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult>;

    /// Does this layer only contain some data for the segment (incremental),
    /// or does it contain a version of every page? This is important to know
    /// for garbage collecting old layers: an incremental layer depends on
    /// the previous non-incremental layer.
    fn is_incremental(&self) -> bool;

    /// Returns true for layers that are represented in memory.
    fn is_in_memory(&self) -> bool;

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + '_>;
    
    /// Return a set of all distinct Keys present in this layer
    fn collect_keys(&self, key_range: &Range<Key>, keys: &mut HashSet<Key>) -> Result<()>;

    /// Release memory used by this layer. There is no corresponding 'load'
    /// function, that's done implicitly when you call one of the get-functions.
    fn unload(&self) -> Result<()>;

    /// Permanently remove this layer from disk.
    fn delete(&self) -> Result<()>;

    /// Dump summary of the contents of the layer to stdout
    fn dump(&self) -> Result<()>;
}
