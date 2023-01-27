//! Common traits and structs for layers
//!
//! A Layer contains all data in a "rectangle" consisting of a range of keys and
//! range of LSNs.
//!
//! There are two kinds of layers, in-memory and on-disk layers. In-memory
//! layers are used to ingest incoming WAL, and provide fast access to the
//! recent page versions. On-disk layers are stored as files on disk, and are
//! immutable. This trait presents the common functionality of in-memory and
//! on-disk layers.
//!
//! Furthermore, there are two kinds of on-disk layers: delta and image layers.
//! A delta layer contains all modifications within a range of LSNs and keys.
//! An image layer is a snapshot of all the data in a key-range, at a single
//! LSN.
//! Both on-disk layers are considered as hstoric ones,
//! since contain history of the database, persisted into the layer files.
//! The layer files could be present or not on the local disk, might get
//! downloaded from the remote storage, if needed.

mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod remote_layer;

use crate::repository::{Key, Value};
use crate::walrecord::NeonWalRecord;
use anyhow::Context;
use bytes::Bytes;
use std::ops::{Deref, Range};
use std::path::PathBuf;
use std::sync::Arc;
use utils::id::TimelineId;

use utils::lsn::Lsn;

pub use delta_layer::{DeltaLayer, DeltaLayerWriter};
pub use filename::{DeltaFileName, ImageFileName, LayerFileName, PathOrConf};
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
/// If 'get_value_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same ValueReconstructState struct in the next 'get_value_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug)]
pub struct ValueReconstructState {
    pub records: Vec<(Lsn, NeonWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,
}

/// Return value from LayerRange::get_value_reconstruct_data
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

/// Basic trait of every layer, describing which [`Key`] and [`Lsn`] ranges this layer covers.
pub trait LayerRange {
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

    /// A short ID string that uniquely identifies the given layer within a [`LayerMap`].
    fn short_id(&self) -> String;
}

/// A trait of a layer that has a corresponding file locally in FS.
pub trait LayerFile {
    fn layer_name(&self) -> LayerFileName;

    fn local_path(&self) -> PathBuf;

    fn file_size(&self) -> u64;

    fn delete(&self) -> anyhow::Result<()> {
        let path_to_delete = self.local_path();
        std::fs::remove_file(&path_to_delete)
            .with_context(|| format!("Failed to remove local layer file {path_to_delete:?}"))
    }
}

/// A trait of a layer that has its contents available for reading.
pub trait LayerContent {
    /// Dump summary of the contents of the layer to stdout
    fn dump(&self, verbose: bool) -> anyhow::Result<()>;

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
    ) -> anyhow::Result<ValueReconstructResult>;
}

/// Returned by [`Layer::iter`]
pub type LayerIter<'i> = Box<dyn Iterator<Item = anyhow::Result<(Key, Lsn, Value)>> + 'i>;

/// Returned by [`Layer::key_iter`]
pub type LayerKeyIter<'i> = Box<dyn Iterator<Item = (Key, Lsn, u64)> + 'i>;

/// A layer that once had an FS file reprentation, with historical data.
/// Could contain remote-only layers, that were once evicted from the local storage.
pub enum HistoricLayer<D = DeltaLayer, I = ImageLayer> {
    /// L0 layer, gets created on disk after initial in-memory data gets flushed.
    Delta(LocalOrRemote<D>),
    /// L1 layer, gets compacted from L0 layer(s) on disk, replacing them.
    Image(LocalOrRemote<I>),
}

impl<D, I> PartialEq for HistoricLayer<D, I> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Delta(l0), Self::Delta(r0)) => l0 == r0,
            (Self::Image(l0), Self::Image(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl<D, I> Clone for HistoricLayer<D, I> {
    fn clone(&self) -> Self {
        match self {
            Self::Delta(d) => Self::Delta(d.clone()),
            Self::Image(i) => Self::Image(i.clone()),
        }
    }
}

/// A layer, that might need downloading before most of its [meta]data is accesible.
// TODO: `RemoteLayer` has a separation of delta/image inside, but local layers are separated with L
// find a better way to unify this, or a better place for this separation.
pub enum LocalOrRemote<L> {
    Local(Arc<L>),
    Remote(Arc<RemoteLayer>),
}

impl<L> LocalOrRemote<L> {
    fn as_remote(&self) -> Option<&Arc<RemoteLayer>> {
        match self {
            Self::Local(_) => None,
            Self::Remote(remote_layer) => Some(remote_layer),
        }
    }
}

impl<L> LocalOrRemote<L>
where
    L: LayerFile,
{
    pub fn layer_name(&self) -> LayerFileName {
        match self {
            Self::Local(l) => l.layer_name(),
            Self::Remote(r) => r.layer_name(),
        }
    }
}

// Currently, we compact only `DeltaLayer`, creating `ImageLayer` out of them, ergo
// current impl is needed for delta layers only.
// This might serve as a base for another trait, if the same compaction approach is used for image layers.
impl LocalOrRemote<DeltaLayer> {
    /// Iterate through all keys and values stored in the layer
    pub fn iter(&self) -> anyhow::Result<LayerIter<'_>> {
        match self {
            Self::Local(local_delta_layer) => local_delta_layer.iter(),
            Self::Remote(_remote) => anyhow::bail!("cannot iterate a remote layer"),
        }
    }

    /// Iterate through all keys stored in the layer. Returns key, lsn and value size
    /// It is used only for compaction and so is currently implemented only for DeltaLayer
    pub fn key_iter(&self) -> anyhow::Result<LayerKeyIter<'_>> {
        match self {
            Self::Local(local_delta_layer) => local_delta_layer.key_iter(),
            Self::Remote(_remote) => anyhow::bail!("cannot iterate a remote layer"),
        }
    }
}

impl<L> Clone for LocalOrRemote<L> {
    fn clone(&self) -> Self {
        match self {
            Self::Local(l) => Self::Local(Arc::clone(l)),
            Self::Remote(r) => Self::Remote(Arc::clone(r)),
        }
    }
}

impl From<DeltaLayer> for HistoricLayer {
    fn from(delta: DeltaLayer) -> Self {
        Self::Delta(LocalOrRemote::Local(Arc::new(delta)))
    }
}

impl From<ImageLayer> for HistoricLayer {
    fn from(image: ImageLayer) -> Self {
        Self::Image(LocalOrRemote::Local(Arc::new(image)))
    }
}

impl<D, I> From<RemoteLayer> for HistoricLayer<D, I> {
    fn from(remote: RemoteLayer) -> Self {
        match remote.layer_name() {
            LayerFileName::Image(_) => Self::Image(LocalOrRemote::Remote(Arc::new(remote))),
            LayerFileName::Delta(_) => Self::Delta(LocalOrRemote::Remote(Arc::new(remote))),
            #[cfg(test)]
            LayerFileName::Test(_) => unimplemented!(),
        }
    }
}

impl From<Arc<RemoteLayer>> for HistoricLayer {
    fn from(remote: Arc<RemoteLayer>) -> Self {
        match remote.layer_name() {
            LayerFileName::Image(_) => Self::Image(LocalOrRemote::Remote(remote)),
            LayerFileName::Delta(_) => Self::Delta(LocalOrRemote::Remote(remote)),
            #[cfg(test)]
            LayerFileName::Test(_) => unimplemented!(),
        }
    }
}

impl<D, I> HistoricLayer<D, I> {
    pub fn is_l0(&self) -> bool {
        matches!(self, Self::Delta(_))
    }
}

pub enum ValueReconstructError {
    LayerMissing(Arc<RemoteLayer>),
    Reconstruct(anyhow::Error),
}

impl<D, I> HistoricLayer<D, I>
where
    D: LayerFile,
    I: LayerFile,
{
    /// File name used for this layer, both in the pageserver's local filesystem
    /// state as well as in the remote storage.
    pub fn filename(&self) -> LayerFileName {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => delta.layer_name(),
            Self::Image(Local(image)) => image.layer_name(),
            Self::Delta(Remote(remote)) | Self::Image(Remote(remote)) => remote.layer_name(),
        }
    }

    pub fn local_path(&self) -> Option<PathBuf> {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => Some(delta.local_path()),
            Self::Image(Local(image)) => Some(image.local_path()),
            Self::Delta(Remote(_)) | Self::Image(Remote(_)) => None,
        }
    }

    /// Permanently remove this layer from disk.
    pub fn delete(&self) -> anyhow::Result<()> {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => delta.delete(),
            Self::Image(Local(image)) => image.delete(),
            Self::Delta(Remote(_)) | Self::Image(Remote(_)) => Ok(()),
        }
    }

    /// Returns None if the layer file size is not known.
    ///
    /// Should not change over the lifetime of the layer object because
    /// current_physical_size is computed as the som of this value.
    pub fn file_size(&self) -> Option<u64> {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => Some(delta.file_size()),
            Self::Image(Local(image)) => Some(image.file_size()),
            Self::Delta(Remote(remote)) | Self::Image(Remote(remote)) => {
                remote.layer_metadata.file_size()
            }
        }
    }

    pub fn as_remote_layer(&self) -> Option<&Arc<RemoteLayer>> {
        match self {
            Self::Delta(d) => d.as_remote(),
            Self::Image(i) => i.as_remote(),
        }
    }
}

impl<D, I> HistoricLayer<D, I>
where
    D: LayerContent,
    I: LayerContent,
{
    pub fn dump(&self, verbose: bool) -> anyhow::Result<()> {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => delta.dump(verbose),
            Self::Image(Local(image)) => image.dump(verbose),
            Self::Delta(Remote(remote)) | Self::Image(Remote(remote)) => {
                remote.dump();
                Ok(())
            }
        }
    }

    pub fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult, ValueReconstructError> {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => delta
                .get_value_reconstruct_data(key, lsn_range, reconstruct_data)
                .map_err(ValueReconstructError::Reconstruct),
            Self::Image(Local(image)) => image
                .get_value_reconstruct_data(key, lsn_range, reconstruct_data)
                .map_err(ValueReconstructError::Reconstruct),
            Self::Delta(Remote(remote)) | Self::Image(Remote(remote)) => {
                Err(ValueReconstructError::LayerMissing(Arc::clone(remote)))
            }
        }
    }
}

impl HistoricLayer {
    pub fn timeline_id(&self) -> TimelineId {
        use LocalOrRemote::*;
        match self {
            Self::Delta(Local(delta)) => delta.timeline_id,
            Self::Image(Local(image)) => image.timeline_id,
            Self::Delta(Remote(remote)) | Self::Image(Remote(remote)) => remote.timeline_id(),
        }
    }
}

impl<L> PartialEq for LocalOrRemote<L> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LocalOrRemote::Local(l0), LocalOrRemote::Local(l1)) => Arc::ptr_eq(l0, l1),
            (LocalOrRemote::Remote(r0), LocalOrRemote::Remote(r1)) => Arc::ptr_eq(r0, r1),
            _ => false,
        }
    }
}

impl<L> Deref for LocalOrRemote<L>
where
    L: LayerRange + 'static,
{
    type Target = dyn LayerRange;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Local(l) => l.as_ref(),
            Self::Remote(r) => r.as_ref(),
        }
    }
}

impl<D, I> Deref for HistoricLayer<D, I>
where
    D: LayerRange + 'static,
    I: LayerRange + 'static,
{
    type Target = dyn LayerRange;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Delta(d) => d.deref(),
            Self::Image(i) => i.deref(),
        }
    }
}

/// Holds metadata about a layer without any content. Used mostly for testing.
pub struct LayerDescriptor {
    pub key: Range<Key>,
    pub lsn: Range<Lsn>,
    pub is_incremental: bool,
    pub short_id: String,
}

impl LayerRange for LayerDescriptor {
    fn get_key_range(&self) -> Range<Key> {
        self.key.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn.clone()
    }

    fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    fn short_id(&self) -> String {
        self.short_id.clone()
    }
}
