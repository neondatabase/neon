//! Synthetic size calculation
#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]

mod calculation;
pub mod svg;

/// StorageModel is the input to the synthetic size calculation.
///
/// It represents a tree of timelines, with just the information that's needed
/// for the calculation. This doesn't track timeline names or where each timeline
/// begins and ends, for example. Instead, it consists of "points of interest"
/// on the timelines. A point of interest could be the timeline start or end point,
/// the oldest point on a timeline that needs to be retained because of PITR
/// cutoff, or snapshot points named by the user. For each such point, and the
/// edge connecting the points (implicit in Segment), we store information about
/// whether we need to be able to recover to the point, and if known, the logical
/// size at the point.
///
/// The segments must form a well-formed tree, with no loops.
#[derive(serde::Serialize)]
pub struct StorageModel {
    pub segments: Vec<Segment>,
}

/// Segment represents one point in the tree of branches, *and* the edge that leads
/// to it (if any). We don't need separate structs for points and edges, because each
/// point can have only one parent.
///
/// When 'needed' is true, it means that we need to be able to reconstruct
/// any version between 'parent.lsn' and 'lsn'. If you want to represent that only
/// a single point is needed, create two Segments with the same lsn, and mark only
/// the child as needed.
///
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Segment {
    /// Previous segment index into ['Storage::segments`], if any.
    pub parent: Option<usize>,

    /// LSN at this point
    pub lsn: u64,

    /// Logical size at this node, if known.
    pub size: Option<u64>,

    /// If true, the segment from parent to this node is needed by `retention_period`
    pub needed: bool,
}

/// Result of synthetic size calculation. Returned by StorageModel::calculate()
pub struct SizeResult {
    pub total_size: u64,

    // This has same length as the StorageModel::segments vector in the input.
    // Each entry in this array corresponds to the entry with same index in
    // StorageModel::segments.
    pub segments: Vec<SegmentSizeResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SegmentSizeResult {
    pub method: SegmentMethod,
    // calculated size of this subtree, using this method
    pub accum_size: u64,
}

/// Different methods to retain history from a particular state
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SegmentMethod {
    SnapshotHere, // A logical snapshot is needed after this segment
    Wal,          // Keep WAL leading up to this node
    Skipped,
}
