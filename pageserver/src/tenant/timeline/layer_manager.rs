use anyhow::{bail, ensure, Context, Result};
use std::{collections::HashMap, sync::Arc};
use tracing::trace;
use utils::{
    id::{TenantId, TimelineId},
    lsn::{AtomicLsn, Lsn},
};

use crate::{
    config::PageServerConf,
    metrics::TimelineMetrics,
    tenant::{
        layer_map::{BatchedUpdates, LayerMap},
        storage_layer::{
            AsLayerDesc, DeltaLayer, ImageLayer, InMemoryLayer, Layer, PersistentLayer,
            PersistentLayerDesc, PersistentLayerKey, RemoteLayer,
        },
        timeline::compare_arced_layers,
    },
};

/// Provides semantic APIs to manipulate the layer map.
pub struct LayerManager {
    layer_map: LayerMap,
    layer_fmgr: LayerFileManager,
}

/// After GC, the layer map changes will not be applied immediately. Users should manually apply the changes after
/// scheduling deletes in remote client.
pub struct ApplyGcResultGuard<'a>(BatchedUpdates<'a>);

impl ApplyGcResultGuard<'_> {
    pub fn flush(self) {
        self.0.flush();
    }
}

impl LayerManager {
    pub fn create() -> Self {
        Self {
            layer_map: LayerMap::default(),
            layer_fmgr: LayerFileManager::new(),
        }
    }

    pub fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        self.layer_fmgr.get_from_desc(desc)
    }

    /// Get an immutable reference to the layer map.
    ///
    /// We expect users only to be able to get an immutable layer map. If users want to make modifications,
    /// they should use the below semantic APIs. This design makes us step closer to immutable storage state.
    pub fn layer_map(&self) -> &LayerMap {
        &self.layer_map
    }

    /// Get a mutable reference to the layer map. This function will be removed once `flush_frozen_layer`
    /// gets a refactor.
    pub fn layer_map_mut(&mut self) -> &mut LayerMap {
        &mut self.layer_map
    }

    /// Replace layers in the layer file manager, used in evictions and layer downloads.
    pub fn replace_and_verify(
        &mut self,
        expected: Arc<dyn PersistentLayer>,
        new: Arc<dyn PersistentLayer>,
    ) -> Result<()> {
        self.layer_fmgr.replace_and_verify(expected, new)
    }

    /// Called from `load_layer_map`. Initialize the layer manager with:
    /// 1. all on-disk layers
    /// 2. next open layer (with disk disk_consistent_lsn LSN)
    pub fn initialize_local_layers(
        &mut self,
        on_disk_layers: Vec<Arc<dyn PersistentLayer>>,
        next_open_layer_at: Lsn,
    ) {
        let mut updates = self.layer_map.batch_update();
        for layer in on_disk_layers {
            Self::insert_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
        self.layer_map.next_open_layer_at = Some(next_open_layer_at);
    }

    /// Initialize when creating a new timeline, called in `init_empty_layer_map`.
    pub fn initialize_empty(&mut self, next_open_layer_at: Lsn) {
        self.layer_map.next_open_layer_at = Some(next_open_layer_at);
    }

    pub fn initialize_remote_layers(
        &mut self,
        corrupted_local_layers: Vec<Arc<dyn PersistentLayer>>,
        remote_layers: Vec<Arc<RemoteLayer>>,
    ) {
        let mut updates = self.layer_map.batch_update();
        for layer in corrupted_local_layers {
            Self::remove_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        for layer in remote_layers {
            Self::insert_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    /// Open a new writable layer to append data if there is no open layer, otherwise return the current open layer,
    /// called within `get_layer_for_write`.
    pub fn get_layer_for_write(
        &mut self,
        lsn: Lsn,
        last_record_lsn: Lsn,
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
    ) -> Result<Arc<InMemoryLayer>> {
        ensure!(lsn.is_aligned());

        ensure!(
            lsn > last_record_lsn,
            "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})\n{}",
            lsn,
            last_record_lsn,
            std::backtrace::Backtrace::force_capture(),
        );

        // Do we have a layer open for writing already?
        let layer = if let Some(open_layer) = &self.layer_map.open_layer {
            if open_layer.get_lsn_range().start > lsn {
                bail!(
                    "unexpected open layer in the future: open layers starts at {}, write lsn {}",
                    open_layer.get_lsn_range().start,
                    lsn
                );
            }

            Arc::clone(open_layer)
        } else {
            // No writeable layer yet. Create one.
            let start_lsn = self
                .layer_map
                .next_open_layer_at
                .context("No next open layer found")?;

            trace!(
                "creating in-memory layer at {}/{} for record at {}",
                timeline_id,
                start_lsn,
                lsn
            );

            let new_layer = InMemoryLayer::create(conf, timeline_id, tenant_id, start_lsn)?;
            let layer = Arc::new(new_layer);

            self.layer_map.open_layer = Some(layer.clone());
            self.layer_map.next_open_layer_at = None;

            layer
        };

        Ok(layer)
    }

    /// Called from `freeze_inmem_layer`, returns true if successfully frozen.
    pub fn try_freeze_in_memory_layer(
        &mut self,
        Lsn(last_record_lsn): Lsn,
        last_freeze_at: &AtomicLsn,
    ) {
        let end_lsn = Lsn(last_record_lsn + 1);

        if let Some(open_layer) = &self.layer_map.open_layer {
            let open_layer_rc = Arc::clone(open_layer);
            // Does this layer need freezing?
            open_layer.freeze(end_lsn);

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            self.layer_map.frozen_layers.push_back(open_layer_rc);
            self.layer_map.open_layer = None;
            self.layer_map.next_open_layer_at = Some(end_lsn);
            last_freeze_at.store(end_lsn);
        }
    }

    /// Add image layers to the layer map, called from `create_image_layers`.
    pub fn track_new_image_layers(&mut self, image_layers: Vec<ImageLayer>) {
        let mut updates = self.layer_map.batch_update();
        for layer in image_layers {
            Self::insert_historic_layer(Arc::new(layer), &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    /// Flush a frozen layer and add the written delta layer to the layer map.
    pub fn finish_flush_l0_layer(
        &mut self,
        delta_layer: Option<DeltaLayer>,
        frozen_layer_for_check: &Arc<InMemoryLayer>,
    ) {
        let l = self.layer_map.frozen_layers.pop_front();
        let mut updates = self.layer_map.batch_update();

        // Only one thread may call this function at a time (for this
        // timeline). If two threads tried to flush the same frozen
        // layer to disk at the same time, that would not work.
        assert!(compare_arced_layers(&l.unwrap(), frozen_layer_for_check));

        if let Some(delta_layer) = delta_layer {
            Self::insert_historic_layer(Arc::new(delta_layer), &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    /// Called when compaction is completed.
    pub fn finish_compact_l0(
        &mut self,
        layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        compact_from: Vec<Arc<dyn PersistentLayer>>,
        compact_to: Vec<Arc<dyn PersistentLayer>>,
        metrics: &TimelineMetrics,
    ) -> Result<()> {
        let mut updates = self.layer_map.batch_update();
        for l in compact_to {
            Self::insert_historic_layer(l, &mut updates, &mut self.layer_fmgr);
        }
        for l in compact_from {
            // NB: the layer file identified by descriptor `l` is guaranteed to be present
            // in the LayerFileManager because compaction kept holding `layer_removal_cs` the entire
            // time, even though we dropped `Timeline::layers` inbetween.
            Self::delete_historic_layer(
                layer_removal_cs.clone(),
                l,
                &mut updates,
                metrics,
                &mut self.layer_fmgr,
            )?;
        }
        updates.flush();
        Ok(())
    }

    /// Called when garbage collect the timeline. Returns a guard that will apply the updates to the layer map.
    pub fn finish_gc_timeline(
        &mut self,
        layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        gc_layers: Vec<Arc<dyn PersistentLayer>>,
        metrics: &TimelineMetrics,
    ) -> Result<ApplyGcResultGuard> {
        let mut updates = self.layer_map.batch_update();
        for doomed_layer in gc_layers {
            Self::delete_historic_layer(
                layer_removal_cs.clone(),
                doomed_layer,
                &mut updates,
                metrics,
                &mut self.layer_fmgr,
            )?; // FIXME: schedule succeeded deletions in timeline.rs `gc_timeline` instead of in batch?
        }
        Ok(ApplyGcResultGuard(updates))
    }

    /// Helper function to insert a layer into the layer map and file manager.
    fn insert_historic_layer(
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        mapping: &mut LayerFileManager,
    ) {
        updates.insert_historic(layer.layer_desc().clone());
        mapping.insert(layer);
    }

    /// Helper function to remove a layer into the layer map and file manager
    fn remove_historic_layer(
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        mapping: &mut LayerFileManager,
    ) {
        updates.remove_historic(layer.layer_desc().clone());
        mapping.remove(layer);
    }

    /// Removes the layer from local FS (if present) and from memory.
    /// Remote storage is not affected by this operation.
    fn delete_historic_layer(
        // we cannot remove layers otherwise, since gc and compaction will race
        _layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        metrics: &TimelineMetrics,
        mapping: &mut LayerFileManager,
    ) -> anyhow::Result<()> {
        if !layer.is_remote_layer() {
            layer.delete_resident_layer_file()?;
            let layer_file_size = layer.file_size();
            metrics.resident_physical_size_gauge.sub(layer_file_size);
        }

        // TODO Removing from the bottom of the layer map is expensive.
        //      Maybe instead discard all layer map historic versions that
        //      won't be needed for page reconstruction for this timeline,
        //      and mark what we can't delete yet as deleted from the layer
        //      map index without actually rebuilding the index.
        updates.remove_historic(layer.layer_desc().clone());
        mapping.remove(layer);

        Ok(())
    }

    pub(crate) fn contains(&self, layer: &Arc<dyn PersistentLayer>) -> bool {
        self.layer_fmgr.contains(layer)
    }
}

pub struct LayerFileManager<T: AsLayerDesc + ?Sized = dyn PersistentLayer>(
    HashMap<PersistentLayerKey, Arc<T>>,
);

impl<T: AsLayerDesc + ?Sized> LayerFileManager<T> {
    fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<T> {
        // The assumption for the `expect()` is that all code maintains the following invariant:
        // A layer's descriptor is present in the LayerMap => the LayerFileManager contains a layer for the descriptor.
        self.0
            .get(&desc.key())
            .with_context(|| format!("get layer from desc: {}", desc.filename()))
            .expect("not found")
            .clone()
    }

    pub(crate) fn insert(&mut self, layer: Arc<T>) {
        let present = self.0.insert(layer.layer_desc().key(), layer.clone());
        if present.is_some() && cfg!(debug_assertions) {
            panic!("overwriting a layer: {:?}", layer.layer_desc())
        }
    }

    pub(crate) fn contains(&self, layer: &Arc<T>) -> bool {
        self.0.contains_key(&layer.layer_desc().key())
    }

    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn remove(&mut self, layer: Arc<T>) {
        let present = self.0.remove(&layer.layer_desc().key());
        if present.is_none() && cfg!(debug_assertions) {
            panic!(
                "removing layer that is not present in layer mapping: {:?}",
                layer.layer_desc()
            )
        }
    }

    pub(crate) fn replace_and_verify(&mut self, expected: Arc<T>, new: Arc<T>) -> Result<()> {
        let key = expected.layer_desc().key();
        let other = new.layer_desc().key();

        let expected_l0 = LayerMap::is_l0(expected.layer_desc());
        let new_l0 = LayerMap::is_l0(new.layer_desc());

        fail::fail_point!("layermap-replace-notfound", |_| anyhow::bail!(
            "layermap-replace-notfound"
        ));

        anyhow::ensure!(
            key == other,
            "expected and new layer have different keys: {key:?} != {other:?}"
        );

        anyhow::ensure!(
            expected_l0 == new_l0,
            "one layer is l0 while the other is not: {expected_l0} != {new_l0}"
        );

        if let Some(layer) = self.0.get_mut(&key) {
            anyhow::ensure!(
                compare_arced_layers(&expected, layer),
                "another layer was found instead of expected, expected={expected:?}, new={new:?}",
                expected = Arc::as_ptr(&expected),
                new = Arc::as_ptr(layer),
            );
            *layer = new;
            Ok(())
        } else {
            anyhow::bail!("layer was not found");
        }
    }
}
