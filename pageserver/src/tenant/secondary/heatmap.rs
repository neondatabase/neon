use std::time::SystemTime;

use crate::tenant::{
    remote_timeline_client::index::IndexLayerMetadata, storage_layer::LayerFileName,
};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr, TimestampSeconds};

use utils::{generation::Generation, id::TimelineId};

#[derive(Serialize, Deserialize)]
pub(super) struct HeatMapTenant {
    /// Generation of the attached location that uploaded the heatmap: this is not required
    /// for correctness, but acts as a hint to secondary locations in order to detect thrashing
    /// in the unlikely event that two attached locations are both uploading conflicting heatmaps.
    pub(super) generation: Generation,

    pub(super) timelines: Vec<HeatMapTimeline>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(crate) struct HeatMapTimeline {
    #[serde_as(as = "DisplayFromStr")]
    pub(super) timeline_id: TimelineId,

    pub(super) layers: Vec<HeatMapLayer>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(crate) struct HeatMapLayer {
    pub(super) name: LayerFileName,
    pub(super) metadata: IndexLayerMetadata,

    #[serde_as(as = "TimestampSeconds<i64>")]
    pub(super) access_time: SystemTime,
    // TODO: an actual 'heat' score that would let secondary locations prioritize downloading
    // the hottest layers, rather than trying to simply mirror whatever layers are on-disk on the primary.
}

impl HeatMapLayer {
    pub(crate) fn new(
        name: LayerFileName,
        metadata: IndexLayerMetadata,
        access_time: SystemTime,
    ) -> Self {
        Self {
            name,
            metadata,
            access_time,
        }
    }
}

impl HeatMapTimeline {
    pub(crate) fn new(timeline_id: TimelineId, layers: Vec<HeatMapLayer>) -> Self {
        Self {
            timeline_id,
            layers,
        }
    }
}
