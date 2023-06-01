use std::ops::Range;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::repository::Key;

use super::{DeltaFileName, ImageFileName, LayerFileName};

/// A unique identifier of a persistent layer. This is different from `LayerDescriptor`, which is only used in the
/// benchmarks. This struct contains all necessary information to find the image / delta layer. It also provides
/// a unified way to generate layer information like file name.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PersistentLayerDesc {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key_range: Range<Key>,
    /// For image layer, this is `[lsn, lsn+1)`.
    pub lsn_range: Range<Lsn>,
    /// Whether this is a delta layer.
    pub is_delta: bool,
    /// Whether this layer only contains part of the keys in the range. In the current implementation, this should
    /// always be equal to `is_delta`. If we land the partial image layer PR someday, image layer could also be
    /// incremental.
    pub is_incremental: bool,
}

impl PersistentLayerDesc {
    pub fn short_id(&self) -> String {
        self.filename().file_name()
    }

    pub fn new_img(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn: Lsn,
        is_incremental: bool,
    ) -> Self {
        Self {
            tenant_id,
            timeline_id,
            key_range,
            lsn_range: Self::image_layer_lsn_range(lsn),
            is_delta: false,
            is_incremental,
        }
    }

    pub fn new_delta(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
    ) -> Self {
        Self {
            tenant_id,
            timeline_id,
            key_range,
            lsn_range,
            is_delta: true,
            is_incremental: true,
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
}
