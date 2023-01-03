//! Common traits and structs for layers

mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod remote_layer;

use crate::repository::{Key, Value};
use crate::walrecord::NeonWalRecord;
use anyhow::Result;
use bytes::Bytes;
use std::fs;
use std::ops::{Deref, Range};
use std::path::PathBuf;
use std::sync::Arc;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

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
    fn get_tenant_id(&self) -> TenantId;

    /// Identify the timeline this layer belongs to
    fn get_timeline_id(&self) -> TimelineId;

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
    /// is available. If this returns PageReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data' to
    /// collect more data.
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
    ) -> anyhow::Result<ValueReconstructResult>;

    /// A short ID string that uniquely identifies the given layer within a [`LayerMap`].
    fn short_id(&self) -> String;

    /// Dump summary of the contents of the layer to stdout
    fn dump(&self, verbose: bool) -> anyhow::Result<()>;
}

/// Returned by [`Layer::iter`]
pub type LayerIter<'i> = Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + 'i>;

/// Returned by [`Layer::key_iter`]
pub type LayerKeyIter<'i> = Box<dyn Iterator<Item = (Key, Lsn, u64)> + 'i>;

#[derive(Clone)]
pub enum PersistentLayer {
    Delta(Arc<DeltaLayer>),
    Image(Arc<ImageLayer>),
    Remote(Arc<RemoteLayer>),
}

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

impl PersistentLayer {
    /// File name used for this layer, both in the pageserver's local filesystem
    /// state as well as in the remote storage.
    pub fn filename(&self) -> LayerFileName {
        match self {
            Self::Delta(delta) => delta.layer_name().into(),
            Self::Image(image) => image.layer_name().into(),
            Self::Remote(remote) => remote.layer_name(),
        }
    }

    // Path to the layer file in the local filesystem.
    // `None` for `RemoteLayer`.
    pub fn local_path(&self) -> Option<PathBuf> {
        match self {
            Self::Delta(delta) => Some(delta.path()),
            Self::Image(image) => Some(image.path()),
            Self::Remote(_remote) => None, // TODO kb is this method needed?
        }
    }

    /// Iterate through all keys and values stored in the layer
    pub fn iter(&self) -> anyhow::Result<LayerIter<'_>> {
        match self {
            Self::Delta(delta) => delta.iter(),
            Self::Image(_image) => unimplemented!(),
            Self::Remote(_remote) => anyhow::bail!("cannot iterate a remote layer"), // TODO kb is this method needed?
        }
    }

    /// Iterate through all keys stored in the layer. Returns key, lsn and value size
    /// It is used only for compaction and so is currently implemented only for DeltaLayer
    pub fn key_iter(&self) -> anyhow::Result<LayerKeyIter<'_>> {
        match self {
            Self::Delta(delta) => delta.key_iter(),
            Self::Image(_image) => panic!("Not implemented"),
            Self::Remote(_remote) => anyhow::bail!("cannot iterate a remote layer"), // TODO kb is this method needed?
        }
    }

    /// Permanently remove this layer from disk.
    pub fn delete(&self) -> anyhow::Result<()> {
        match self {
            Self::Delta(delta) => {
                // delete underlying file
                fs::remove_file(delta.path())?;
                Ok(())
            }
            Self::Image(image) => {
                // delete underlying file
                fs::remove_file(image.path())?;
                Ok(())
            }
            Self::Remote(_remote) => Ok(()), // TODO kb is this method needed?
        }
    }

    pub fn is_remote_layer(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    /// Returns None if the layer file size is not known.
    ///
    /// Should not change over the lifetime of the layer object because
    /// current_physical_size is computed as the som of this value.
    pub fn file_size(&self) -> Option<u64> {
        match self {
            Self::Delta(delta) => Some(delta.file_size),
            Self::Image(image) => Some(image.file_size),
            Self::Remote(remote) => remote.layer_metadata.file_size(), // TODO kb is this method needed?
        }
    }
}

impl PartialEq for PersistentLayer {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Delta(d0), Self::Delta(d1)) => Arc::ptr_eq(d0, d1),
            (Self::Image(i0), Self::Image(i1)) => Arc::ptr_eq(i0, i1),
            (Self::Remote(r0), Self::Remote(r1)) => Arc::ptr_eq(r0, r1),
            _ => false,
        }
    }
}

impl Deref for PersistentLayer {
    type Target = dyn Layer;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Delta(x) => x.as_ref(),
            Self::Image(x) => x.as_ref(),
            Self::Remote(x) => x.as_ref(),
        }
    }
}
