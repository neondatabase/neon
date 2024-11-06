use core::fmt::Display;
use pageserver_api::shard::TenantShardId;
use std::ops::Range;
use utils::{id::TimelineId, lsn::Lsn};

use pageserver_api::key::Key;

use super::{DeltaLayerName, ImageLayerName, LayerName};

use serde::{Deserialize, Serialize};

#[cfg(test)]
use utils::id::TenantId;

/// A unique identifier of a persistent layer.
///
/// This is different from `LayerDescriptor`, which is only used in the benchmarks.
/// This struct contains all necessary information to find the image / delta layer. It also provides
/// a unified way to generate layer information like file name.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Hash)]
pub struct PersistentLayerDesc {
    pub tenant_shard_id: TenantShardId,
    pub timeline_id: TimelineId,
    /// Range of keys that this layer covers
    pub key_range: Range<Key>,
    /// Inclusive start, exclusive end of the LSN range that this layer holds.
    ///
    /// - For an open in-memory layer, the end bound is MAX_LSN
    /// - For a frozen in-memory layer or a delta layer, the end bound is a valid lsn after the
    ///   range start
    /// - An image layer represents snapshot at one LSN, so end_lsn is always the snapshot LSN + 1
    pub lsn_range: Range<Lsn>,
    /// Whether this is a delta layer, and also, is this incremental.
    pub is_delta: bool,
    pub file_size: u64,
}

/// A unique identifier of a persistent layer within the context of one timeline.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct PersistentLayerKey {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
    pub is_delta: bool,
}

impl std::fmt::Display for PersistentLayerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}..{} {}..{} is_delta={}",
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end,
            self.is_delta
        )
    }
}

impl From<ImageLayerName> for PersistentLayerKey {
    fn from(image_layer_name: ImageLayerName) -> Self {
        Self {
            key_range: image_layer_name.key_range,
            lsn_range: PersistentLayerDesc::image_layer_lsn_range(image_layer_name.lsn),
            is_delta: false,
        }
    }
}

impl From<DeltaLayerName> for PersistentLayerKey {
    fn from(delta_layer_name: DeltaLayerName) -> Self {
        Self {
            key_range: delta_layer_name.key_range,
            lsn_range: delta_layer_name.lsn_range,
            is_delta: true,
        }
    }
}

impl From<LayerName> for PersistentLayerKey {
    fn from(layer_name: LayerName) -> Self {
        match layer_name {
            LayerName::Image(i) => i.into(),
            LayerName::Delta(d) => d.into(),
        }
    }
}
impl PersistentLayerDesc {
    pub fn key(&self) -> PersistentLayerKey {
        PersistentLayerKey {
            key_range: self.key_range.clone(),
            lsn_range: self.lsn_range.clone(),
            is_delta: self.is_delta,
        }
    }

    pub fn short_id(&self) -> impl Display {
        self.layer_name()
    }

    #[cfg(test)]
    pub fn new_test(key_range: Range<Key>, lsn_range: Range<Lsn>, is_delta: bool) -> Self {
        Self {
            tenant_shard_id: TenantShardId::unsharded(TenantId::generate()),
            timeline_id: TimelineId::generate(),
            key_range,
            lsn_range,
            is_delta,
            file_size: 0,
        }
    }

    pub fn new_img(
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn: Lsn,
        file_size: u64,
    ) -> Self {
        Self {
            tenant_shard_id,
            timeline_id,
            key_range,
            lsn_range: Self::image_layer_lsn_range(lsn),
            is_delta: false,
            file_size,
        }
    }

    pub fn new_delta(
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
        file_size: u64,
    ) -> Self {
        Self {
            tenant_shard_id,
            timeline_id,
            key_range,
            lsn_range,
            is_delta: true,
            file_size,
        }
    }

    pub fn from_filename(
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        filename: LayerName,
        file_size: u64,
    ) -> Self {
        match filename {
            LayerName::Image(i) => {
                Self::new_img(tenant_shard_id, timeline_id, i.key_range, i.lsn, file_size)
            }
            LayerName::Delta(d) => Self::new_delta(
                tenant_shard_id,
                timeline_id,
                d.key_range,
                d.lsn_range,
                file_size,
            ),
        }
    }

    /// Get the LSN that the image layer covers.
    pub fn image_layer_lsn(&self) -> Lsn {
        assert!(!self.is_delta);
        assert!(self.lsn_range.start + 1 == self.lsn_range.end);
        self.lsn_range.start
    }

    /// Get the LSN range corresponding to a single image layer LSN.
    pub fn image_layer_lsn_range(lsn: Lsn) -> Range<Lsn> {
        lsn..(lsn + 1)
    }

    /// Get a delta layer name for this layer.
    ///
    /// Panic: if this is not a delta layer.
    pub fn delta_layer_name(&self) -> DeltaLayerName {
        assert!(self.is_delta);
        DeltaLayerName {
            key_range: self.key_range.clone(),
            lsn_range: self.lsn_range.clone(),
        }
    }

    /// Get a image layer name for this layer.
    ///
    /// Panic: if this is not an image layer, or the lsn range is invalid
    pub fn image_layer_name(&self) -> ImageLayerName {
        assert!(!self.is_delta);
        assert!(self.lsn_range.start + 1 == self.lsn_range.end);
        ImageLayerName {
            key_range: self.key_range.clone(),
            lsn: self.lsn_range.start,
        }
    }

    pub fn layer_name(&self) -> LayerName {
        if self.is_delta {
            self.delta_layer_name().into()
        } else {
            self.image_layer_name().into()
        }
    }

    // TODO: remove this in the future once we refactor timeline APIs.

    pub fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn_range.clone()
    }

    pub fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    pub fn get_timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

    /// Does this layer only contain some data for the key-range (incremental),
    /// or does it contain a version of every page? This is important to know
    /// for garbage collecting old layers: an incremental layer depends on
    /// the previous non-incremental layer.
    pub fn is_incremental(&self) -> bool {
        self.is_delta
    }

    pub fn is_delta(&self) -> bool {
        self.is_delta
    }

    pub fn dump(&self) {
        if self.is_delta {
            println!(
                "----- delta layer for ten {} tli {} keys {}-{} lsn {}-{} is_incremental {} size {} ----",
                self.tenant_shard_id,
                self.timeline_id,
                self.key_range.start,
                self.key_range.end,
                self.lsn_range.start,
                self.lsn_range.end,
                self.is_incremental(),
                self.file_size,
            );
        } else {
            println!(
                "----- image layer for ten {} tli {} key {}-{} at {} is_incremental {} size {} ----",
                self.tenant_shard_id,
                self.timeline_id,
                self.key_range.start,
                self.key_range.end,
                self.image_layer_lsn(),
                self.is_incremental(),
                self.file_size
            );
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}
