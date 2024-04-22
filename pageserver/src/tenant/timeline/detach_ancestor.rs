use std::sync::Arc;

use utils::lsn::Lsn;

use super::{layer_manager::LayerManager, DetachFromAncestorError, Timeline};
use crate::{
    context::RequestContext,
    tenant::{
        remote_timeline_client::RemoteTimelineClient,
        storage_layer::{AsLayerDesc as _, DeltaLayerWriter, Layer, ResidentLayer},
    },
};

pub(super) fn partition_work(
    ancestor_lsn: Lsn,
    source_layermap: &LayerManager,
) -> (usize, Vec<Layer>, Vec<Layer>) {
    let mut straddling_branchpoint = vec![];
    let mut rest_of_historic = vec![];

    let mut later_by_lsn = 0;

    for desc in source_layermap.layer_map().iter_historic_layers() {
        // off by one chances here:
        // - start is inclusive
        // - end is exclusive
        if desc.lsn_range.start > ancestor_lsn {
            later_by_lsn += 1;
            continue;
        }

        let target = if desc.lsn_range.start <= ancestor_lsn
            && desc.lsn_range.end > ancestor_lsn
            && desc.is_delta
        {
            // TODO: image layer at Lsn optimization
            &mut straddling_branchpoint
        } else {
            &mut rest_of_historic
        };

        target.push(source_layermap.get_from_desc(&desc));
    }

    (later_by_lsn, straddling_branchpoint, rest_of_historic)
}

pub(super) fn retain_missing_layers(historic: &mut Vec<Layer>, target_layermap: &LayerManager) {
    // we can safely not copy the layers again which happen to be found in the index_part.json
    historic.retain(|layer| {
        let desc = layer.layer_desc();

        let key_range = desc.key_range.clone();
        let lsn_range = desc.lsn_range.clone();
        let is_delta = desc.is_delta;

        let key = crate::tenant::storage_layer::PersistentLayerKey {
            key_range,
            lsn_range,
            is_delta,
        };

        target_layermap.get(&key).is_none()
    });
}

pub(super) fn retain_layers_to_copy_lsn_prefix(
    end_lsn: Lsn,
    ancestor_lsn: Lsn,
    straddling: &mut Vec<Layer>,
    target_layermap: &LayerManager,
) {
    straddling.retain(|layer| {
        let desc = layer.layer_desc();
        assert!(desc.is_delta);

        assert!(desc.lsn_range.start <= ancestor_lsn);
        assert!(desc.lsn_range.end > ancestor_lsn);

        let key_range = desc.key_range.clone();
        let lsn_range = desc.lsn_range.start..end_lsn;

        let key = crate::tenant::storage_layer::PersistentLayerKey {
            key_range,
            lsn_range,
            is_delta: true,
        };

        // keep if we haven't already rewritten this
        target_layermap.get(&key).is_none()
    });
}

pub(super) async fn copy_lsn_prefix(
    end_lsn: Lsn,
    layer: &Layer,
    target_timeline: &Arc<Timeline>,
    ctx: &RequestContext,
) -> Result<Option<ResidentLayer>, DetachFromAncestorError> {
    use DetachFromAncestorError::{CopyDeltaPrefix, RewrittenDeltaDownloadFailed};

    tracing::debug!(%layer, %end_lsn, "copying lsn prefix");

    let mut writer = DeltaLayerWriter::new(
        target_timeline.conf,
        target_timeline.timeline_id,
        target_timeline.tenant_shard_id,
        layer.layer_desc().key_range.start,
        layer.layer_desc().lsn_range.start..end_lsn,
    )
    .await
    .map_err(CopyDeltaPrefix)?;

    // likely shutdown
    let resident = layer
        .download_and_keep_resident()
        .await
        .map_err(RewrittenDeltaDownloadFailed)?;

    let records = resident
        .copy_delta_prefix(&mut writer, end_lsn, ctx)
        .await
        .map_err(CopyDeltaPrefix)?;

    drop(resident);

    tracing::debug!(%layer, records, "copied records");

    if records == 0 {
        drop(writer);
        // TODO: we might want to store an empty marker in remote storage for this
        // layer so that we will not needlessly walk `layer` on repeated attempts.
        Ok(None)
    } else {
        // reuse the key instead of adding more holes between layers by using the real
        // highest key in the layer.
        let reused_highest_key = layer.layer_desc().key_range.end;
        let copied = writer
            .finish(reused_highest_key, target_timeline)
            .await
            .map_err(CopyDeltaPrefix)?;

        tracing::debug!(%layer, %copied, "new layer produced");

        Ok(Some(copied))
    }
}

pub(super) fn schedule_remote_copy(
    rtc: &Arc<RemoteTimelineClient>,
    adopted: &Layer,
    adoptee: &Arc<Timeline>,
) -> Result<Layer, DetachFromAncestorError> {
    use DetachFromAncestorError::ShuttingDown;

    // depending if Layer::keep_resident we could hardlink

    let owned = crate::tenant::storage_layer::Layer::for_evicted(
        adoptee.conf,
        adoptee,
        adopted.layer_desc().filename(),
        // generation is per tenant, we don't have any values to change here
        adopted.metadata(),
    );
    rtc.schedule_layer_adoption(adopted.clone(), owned.clone())
        .map_err(|_| ShuttingDown)?;

    Ok(owned)
}
