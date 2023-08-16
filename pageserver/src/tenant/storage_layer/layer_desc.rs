use anyhow::Result;
use core::fmt::Display;
use std::ops::Range;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::{context::RequestContext, repository::Key};

use super::{DeltaFileName, ImageFileName, LayerFileName};

use serde::{Deserialize, Serialize};

/// A unique identifier of a persistent layer. This is different from `LayerDescriptor`, which is only used in the
/// benchmarks. This struct contains all necessary information to find the image / delta layer. It also provides
/// a unified way to generate layer information like file name.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct PersistentLayerDesc {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key_range: Range<Key>,
    /// For image layer, this is `[lsn, lsn+1)`.
    pub lsn_range: Range<Lsn>,
    /// Whether this is a delta layer.
    pub is_delta: bool,
    /// Whether this layer only contains page images for part of the keys in the range. In the current implementation, this should
    /// always be equal to `is_delta`. If we land the partial image layer PR someday, image layer could also be
    /// incremental.
    pub is_incremental: bool,
    /// File size
    pub file_size: u64,
}

/// A unique identifier of a persistent layer within the context of one timeline.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct PersistentLayerKey {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
    pub is_delta: bool,
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
        self.filename()
    }

    #[cfg(test)]
    pub fn new_test(key_range: Range<Key>) -> Self {
        Self {
            tenant_id: TenantId::generate(),
            timeline_id: TimelineId::generate(),
            key_range,
            lsn_range: Lsn(0)..Lsn(1),
            is_delta: false,
            is_incremental: false,
            file_size: 0,
        }
    }

    pub fn new_img(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn: Lsn,
        is_incremental: bool,
        file_size: u64,
    ) -> Self {
        Self {
            tenant_id,
            timeline_id,
            key_range,
            lsn_range: Self::image_layer_lsn_range(lsn),
            is_delta: false,
            is_incremental,
            file_size,
        }
    }

    pub fn new_delta(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
        file_size: u64,
    ) -> Self {
        Self {
            tenant_id,
            timeline_id,
            key_range,
            lsn_range,
            is_delta: true,
            is_incremental: true,
            file_size,
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

    /// Get a delta file name for this layer.
    ///
    /// Panic: if this is not a delta layer.
    pub fn delta_file_name(&self) -> DeltaFileName {
        assert!(self.is_delta);
        DeltaFileName {
            key_range: self.key_range.clone(),
            lsn_range: self.lsn_range.clone(),
        }
    }

    /// Get a delta file name for this layer.
    ///
    /// Panic: if this is not an image layer, or the lsn range is invalid
    pub fn image_file_name(&self) -> ImageFileName {
        assert!(!self.is_delta);
        assert!(self.lsn_range.start + 1 == self.lsn_range.end);
        ImageFileName {
            key_range: self.key_range.clone(),
            lsn: self.lsn_range.start,
        }
    }

    pub fn filename(&self) -> LayerFileName {
        if self.is_delta {
            self.delta_file_name().into()
        } else {
            self.image_file_name().into()
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

    pub fn get_tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub fn is_incremental(&self) -> bool {
        self.is_incremental
    }

    pub fn is_delta(&self) -> bool {
        self.is_delta
    }

    pub fn dump(&self, _verbose: bool, _ctx: &RequestContext) -> Result<()> {
        println!(
            "----- layer for ten {} tli {} keys {}-{} lsn {}-{} is_delta {} is_incremental {} size {} ----",
            self.tenant_id,
            self.timeline_id,
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end,
            self.is_delta,
            self.is_incremental,
            self.file_size,
        );

        Ok(())
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}
