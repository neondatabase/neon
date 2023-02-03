//! Common traits and structs for layers

pub mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod remote_layer;

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::repository::{Key, Value};
use crate::walrecord::NeonWalRecord;
use anyhow::Result;
use bytes::Bytes;
use pageserver_api::models::HistoricLayerInfo;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub use delta_layer::{DeltaLayer, DeltaLayerWriter};
pub use filename::{DeltaFileName, ImageFileName, LayerFileName};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use remote_layer::RemoteLayer;

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

pub fn range_eq<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialEq<T>,
{
    a.start == b.start && a.end == b.end
}

/// Struct used to communicate across calls to 'get_value_reconstruct_data'.
///
/// Before first call, you can fill in 'page_img' if you have an older cached
/// version of the page available. That can save work in
/// 'get_value_reconstruct_data', as it can stop searching for page versions
/// when all the WAL records going back to the cached image have been collected.
///
/// When get_value_reconstruct_data returns Complete, 'img' is set to an image
/// of the page, or the oldest WAL record in 'records' is a will_init-type
/// record that initializes the page without requiring a previous image.
///
/// If 'get_page_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same ValueReconstructState struct in the next 'get_value_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug)]
pub struct ValueReconstructState {
    pub records: Vec<(Lsn, NeonWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,
}

/// Return value from Layer::get_page_reconstruct_data
#[derive(Clone, Copy, Debug)]
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

/// Supertrait of the [`Layer`] trait that captures the bare minimum interface
/// required by [`LayerMap`].
pub trait Layer: Send + Sync {
    /// Range of keys that this layer covers
    fn get_key_range(&self) -> Range<Key>;

    /// Inclusive start bound of the LSN range that this layer holds
    /// Exclusive end bound of the LSN range that this layer holds.
    ///
    /// - For an open in-memory layer, this is MAX_LSN.
    /// - For a frozen in-memory layer or a delta layer, this is a valid end bound.
    /// - An image layer represents snapshot at one LSN, so end_lsn is always the snapshot LSN + 1
    fn get_lsn_range(&self) -> Range<Lsn>;

    /// Does this layer only contain some data for the key-range (incremental),
    /// or does it contain a version of every page? This is important to know
    /// for garbage collecting old layers: an incremental layer depends on
    /// the previous non-incremental layer.
    fn is_incremental(&self) -> bool;

    ///
    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// See PageReconstructResult for possible return values. The collected data
    /// is appended to reconstruct_data; the caller should pass an empty struct
    /// on first call, or a struct with a cached older image of the page if one
    /// is available. If this returns ValueReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data' to
    /// collect more data.
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> Result<ValueReconstructResult>;

    /// A short ID string that uniquely identifies the given layer within a [`LayerMap`].
    fn short_id(&self) -> String;

    /// Dump summary of the contents of the layer to stdout
    fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()>;
}

/// Returned by [`Layer::iter`]
pub type LayerIter<'i> = Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + 'i>;

/// Returned by [`Layer::key_iter`]
pub type LayerKeyIter<'i> = Box<dyn Iterator<Item = (Key, Lsn, u64)> + 'i>;

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This trait presents the common functionality of in-memory and
/// on-disk layers.
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN
///
pub trait PersistentLayer: Layer {
    fn get_tenant_id(&self) -> TenantId;

    /// Identify the timeline this layer belongs to
    fn get_timeline_id(&self) -> TimelineId;

    /// File name used for this layer, both in the pageserver's local filesystem
    /// state as well as in the remote storage.
    fn filename(&self) -> LayerFileName;

    // Path to the layer file in the local filesystem.
    // `None` for `RemoteLayer`.
    fn local_path(&self) -> Option<PathBuf>;

    /// Iterate through all keys and values stored in the layer
    fn iter(&self, ctx: &RequestContext) -> Result<LayerIter<'_>>;

    /// Iterate through all keys stored in the layer. Returns key, lsn and value size
    /// It is used only for compaction and so is currently implemented only for DeltaLayer
    fn key_iter(&self, _ctx: &RequestContext) -> Result<LayerKeyIter<'_>> {
        panic!("Not implemented")
    }

    /// Permanently remove this layer from disk.
    fn delete(&self) -> Result<()>;

    fn downcast_remote_layer(self: Arc<Self>) -> Option<std::sync::Arc<RemoteLayer>> {
        None
    }

    fn is_remote_layer(&self) -> bool {
        false
    }

    /// Returns None if the layer file size is not known.
    ///
    /// Should not change over the lifetime of the layer object because
    /// current_physical_size is computed as the som of this value.
    fn file_size(&self) -> Option<u64>;

    fn info(&self) -> HistoricLayerInfo;
}

pub fn downcast_remote_layer(
    layer: &Arc<dyn PersistentLayer>,
) -> Option<std::sync::Arc<RemoteLayer>> {
    if layer.is_remote_layer() {
        Arc::clone(layer).downcast_remote_layer()
    } else {
        None
    }
}

impl std::fmt::Debug for dyn Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Layer")
            .field("short_id", &self.short_id())
            .finish()
    }
}

/// Holds metadata about a layer without any content. Used mostly for testing.
///
/// To use filenames as fixtures, parse them as [`LayerFileName`] then convert from that to a
/// LayerDescriptor.
#[derive(Clone)]
pub struct LayerDescriptor {
    pub key: Range<Key>,
    pub lsn: Range<Lsn>,
    pub is_incremental: bool,
    pub short_id: String,
}

impl Layer for LayerDescriptor {
    fn get_key_range(&self) -> Range<Key> {
        self.key.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn.clone()
    }

    fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_data: &mut ValueReconstructState,
        _ctx: &RequestContext,
    ) -> Result<ValueReconstructResult> {
        todo!("This method shouldn't be part of the Layer trait")
    }

    fn short_id(&self) -> String {
        self.short_id.clone()
    }

    fn dump(&self, _verbose: bool, _ctx: &RequestContext) -> Result<()> {
        todo!()
    }
}

impl From<DeltaFileName> for LayerDescriptor {
    fn from(value: DeltaFileName) -> Self {
        let short_id = value.to_string();
        LayerDescriptor {
            key: value.key_range,
            lsn: value.lsn_range,
            is_incremental: true,
            short_id,
        }
    }
}

impl From<ImageFileName> for LayerDescriptor {
    fn from(value: ImageFileName) -> Self {
        let short_id = value.to_string();
        let lsn = value.lsn_as_range();
        LayerDescriptor {
            key: value.key_range,
            lsn,
            is_incremental: false,
            short_id,
        }
    }
}

impl From<LayerFileName> for LayerDescriptor {
    fn from(value: LayerFileName) -> Self {
        match value {
            LayerFileName::Delta(d) => Self::from(d),
            LayerFileName::Image(i) => Self::from(i),
        }
    }
}

/// Helper enum to hold a PageServerConf, or a path
///
/// This is used by DeltaLayer and ImageLayer. Normally, this holds a reference to the
/// global config, and paths to layer files are constructed using the tenant/timeline
/// path from the config. But in the 'pageserver_binutils' binary, we need to construct a Layer
/// struct for a file on disk, without having a page server running, so that we have no
/// config. In that case, we use the Path variant to hold the full path to the file on
/// disk.
enum PathOrConf {
    Path(PathBuf),
    Conf(&'static PageServerConf),
}
