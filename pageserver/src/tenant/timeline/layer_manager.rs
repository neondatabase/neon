use anyhow::{bail, ensure, Context, Result};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use either::Either;
use std::sync::Arc;
use tracing::{log::warn, trace};
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
    /// The layer map that tracks all layers in the timeline at the current time.
    layer_map: ArcSwap<LayerMap>,
    /// Ensure there is only one thread that can modify the layer map at a time.
    state_lock: Arc<tokio::sync::Mutex<()>>,
    /// Layer file manager that manages the layer files in the local / remote file system.
    layer_fmgr: Arc<LayerFileManager>,
    /// The lock to mock the original behavior of `RwLock<LayerMap>`. Can be removed if
    /// #4509 is implemented.
    pseudo_lock: Arc<tokio::sync::RwLock<()>>,
}

struct LayerSnapshot {
    /// The current snapshot of the layer map. Immutable.
    layer_map: Arc<LayerMap>,
    /// Reference to the file manager. This is mutable and the content might change when
    /// the snapshot is held.
    layer_fmgr: Arc<LayerFileManager>,
}

pub struct LayerManagerReadGuard {
    snapshot: LayerSnapshot,
    /// Mock the behavior of the layer map lock.
    #[allow(dead_code)]
    pseudo_lock: tokio::sync::OwnedRwLockReadGuard<()>,
}

pub struct LayerManagerWriteGuard {
    snapshot: LayerSnapshot,
    /// Semantic layer operations will need to modify the layer content.
    layer_manager: Arc<LayerManager>,
    /// Mock the behavior of the layer map lock.
    #[allow(dead_code)]
    pseudo_lock:
        Either<tokio::sync::OwnedRwLockWriteGuard<()>, tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl LayerManager {
    pub fn create() -> Self {
        Self {
            layer_map: ArcSwap::from(Arc::new(LayerMap::default())),
            state_lock: Arc::new(tokio::sync::Mutex::new(())),
            layer_fmgr: Arc::new(LayerFileManager::new()),
            pseudo_lock: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    /// Take the snapshot of the layer map and return a read guard. The read guard will prevent
    /// the layer map from being modified.
    pub async fn read(&self) -> LayerManagerReadGuard {
        // take the lock before taking snapshot
        let pseudo_lock = self.pseudo_lock.clone().read_owned().await;
        LayerManagerReadGuard {
            snapshot: LayerSnapshot {
                layer_map: self.layer_map.load_full(),
                layer_fmgr: Arc::clone(&self.layer_fmgr),
            },
            pseudo_lock,
        }
    }

    /// Take the snapshot of the layer map and return a write guard. The read guard will prevent
    /// the layer map from being read.
    pub async fn write(self: &Arc<Self>) -> LayerManagerWriteGuard {
        // take the lock before taking snapshot
        let pseudo_lock = self.pseudo_lock.clone().write_owned().await;
        LayerManagerWriteGuard {
            snapshot: LayerSnapshot {
                layer_map: self.layer_map.load_full(),
                layer_fmgr: Arc::clone(&self.layer_fmgr),
            },
            pseudo_lock: Either::Left(pseudo_lock),
            layer_manager: self.clone(),
        }
    }

    /// Take the snapshot of the layer map and return a write guard. With the `modify` call, the guard
    /// will only hold a read lock instead of write lock.
    pub async fn modify(self: &Arc<Self>) -> LayerManagerWriteGuard {
        // take the lock before taking snapshot
        let pseudo_lock = self.pseudo_lock.clone().read_owned().await;
        LayerManagerWriteGuard {
            snapshot: LayerSnapshot {
                layer_map: self.layer_map.load_full(),
                layer_fmgr: Arc::clone(&self.layer_fmgr),
            },
            pseudo_lock: Either::Right(pseudo_lock),
            layer_manager: self.clone(),
        }
    }

    pub fn try_write(
        self: &Arc<Self>,
    ) -> Result<LayerManagerWriteGuard, tokio::sync::TryLockError> {
        self.pseudo_lock
            .clone()
            .try_write_owned()
            .map(|pseudo_lock| LayerManagerWriteGuard {
                snapshot: LayerSnapshot {
                    layer_map: self.layer_map.load_full(),
                    layer_fmgr: Arc::clone(&self.layer_fmgr),
                },
                pseudo_lock: Either::Left(pseudo_lock),
                layer_manager: self.clone(),
            })
    }

    /// Make an update to the layer map. This function will NOT take the state lock and should ONLY
    /// be used when there is not known concurrency when initializing the layer map. Error will be returned only when
    /// the update function fails.
    fn initialize_update(&self, f: impl FnOnce(LayerMap) -> Result<LayerMap>) -> Result<()> {
        let snapshot = self.layer_map.load_full();
        let new_layer_map = f(LayerMap::clone(&*snapshot))?;
        let old_layer_map = self.layer_map.swap(Arc::new(new_layer_map));
        debug_assert_eq!(
            Arc::as_ptr(&snapshot) as usize,
            Arc::as_ptr(&old_layer_map) as usize,
            "race detected when modifying layer map, use `update` instead of `initialize_update`."
        );
        Ok(())
    }

    /// Make an update to the layer map. Error will be returned only when the update function fails.
    async fn update<T>(&self, f: impl FnOnce(LayerMap) -> Result<(LayerMap, T)>) -> Result<T> {
        let _guard = self.state_lock.lock().await;
        let snapshot = self.layer_map.load_full();
        let (new_layer_map, data) = f(LayerMap::clone(&*snapshot))?;
        let old_layer_map = self.layer_map.swap(Arc::new(new_layer_map));
        debug_assert_eq!(
            Arc::as_ptr(&snapshot) as usize,
            Arc::as_ptr(&old_layer_map) as usize,
            "race detected when modifying layer map, please check if `initialize_update` is used on the `update` code path."
        );
        Ok(data)
    }
}

impl LayerSnapshot {
    fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        self.layer_fmgr.get_from_desc(desc)
    }

    /// Get an immutable reference to the layer map.
    ///
    /// If a user needs to modify the layer map, they should get a write guard and use the semantic
    /// functions.
    fn layer_map(&self) -> &LayerMap {
        &self.layer_map
    }

    fn contains(&self, layer: &Arc<dyn PersistentLayer>) -> bool {
        self.layer_fmgr.contains(layer)
    }
}

impl LayerManagerReadGuard {
    pub fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        self.snapshot.get_from_desc(desc)
    }

    /// Get an immutable reference to the layer map.
    pub fn layer_map(&self) -> &LayerMap {
        self.snapshot.layer_map()
    }
}

impl LayerManagerWriteGuard {
    /// Check if the layer file manager contains a layer. This should ONLY be used in the compaction
    /// code path where we need to check if a layer already exists on disk. With the immutable layer
    /// map design, it is possible that the layer map snapshot does not contain the layer, but the layer file
    /// manager does. Therefore, use this function with caution.
    pub(crate) fn contains(&self, layer: &Arc<dyn PersistentLayer>) -> bool {
        self.snapshot.contains(layer)
    }

    pub fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        self.snapshot.get_from_desc(desc)
    }

    /// Get an immutable reference to the layer map.
    pub fn layer_map(&self) -> &LayerMap {
        self.snapshot.layer_map()
    }

    /// Replace layers in the layer file manager, used in evictions and layer downloads.
    pub(crate) fn replace_and_verify(
        &mut self,
        expected: Arc<dyn PersistentLayer>,
        new: Arc<dyn PersistentLayer>,
    ) -> Result<()> {
        self.snapshot.layer_fmgr.replace_and_verify(expected, new)
    }

    /// Called from `load_layer_map`. Initialize the layer manager with:
    /// 1. all on-disk layers
    /// 2. next open layer (with disk disk_consistent_lsn LSN)
    pub(crate) fn initialize_local_layers(
        &mut self,
        on_disk_layers: Vec<Arc<dyn PersistentLayer>>,
        next_open_layer_at: Lsn,
    ) {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );

        self.layer_manager
            .initialize_update(|mut layer_map| {
                let mut updates = layer_map.batch_update();
                for layer in on_disk_layers {
                    Self::insert_historic_layer(layer, &mut updates, &self.snapshot.layer_fmgr);
                }
                updates.flush();
                layer_map.next_open_layer_at = Some(next_open_layer_at);
                Ok(layer_map)
            })
            .unwrap();
    }

    /// Initialize when creating a new timeline, called in `init_empty_layer_map`.
    pub(crate) fn initialize_empty(&mut self, next_open_layer_at: Lsn) {
        self.layer_manager
            .initialize_update(|mut layer_map| {
                layer_map.next_open_layer_at = Some(next_open_layer_at);
                Ok(layer_map)
            })
            .unwrap();
    }

    pub(crate) fn initialize_remote_layers(
        &mut self,
        corrupted_local_layers: Vec<Arc<dyn PersistentLayer>>,
        remote_layers: Vec<Arc<RemoteLayer>>,
    ) {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );

        self.layer_manager
            .initialize_update(|mut layer_map| {
                let mut updates = layer_map.batch_update();
                for layer in corrupted_local_layers {
                    Self::remove_historic_layer(layer, &mut updates, &self.snapshot.layer_fmgr);
                }
                for layer in remote_layers {
                    Self::insert_historic_layer(layer, &mut updates, &self.snapshot.layer_fmgr);
                }
                updates.flush();
                Ok(layer_map)
            })
            .unwrap();
    }

    /// Open a new writable layer to append data if there is no open layer, otherwise return the current open layer,
    /// called within `get_layer_for_write`. Only ONE thread can call this function, which is guaranteed by `write_lock`
    /// in `Timeline`.
    pub(crate) async fn get_layer_for_write(
        &mut self,
        lsn: Lsn,
        last_record_lsn: Lsn,
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
    ) -> Result<Arc<InMemoryLayer>> {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );

        ensure!(lsn.is_aligned());

        ensure!(
            lsn > last_record_lsn,
            "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})\n{}",
            lsn,
            last_record_lsn,
            std::backtrace::Backtrace::force_capture(),
        );

        // Do we have a layer open for writing already?
        let layer = if let Some(open_layer) = &self.snapshot.layer_map.open_layer {
            if open_layer.get_lsn_range().start > lsn {
                bail!(
                    "unexpected open layer in the future: open layers starts at {}, write lsn {}",
                    open_layer.get_lsn_range().start,
                    lsn
                );
            }

            Arc::clone(open_layer)
        } else {
            self.layer_manager
                .update(|mut layer_map| {
                    // No writeable layer yet. Create one.
                    let start_lsn = layer_map
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

                    layer_map.open_layer = Some(layer.clone());
                    layer_map.next_open_layer_at = None;

                    Ok((layer_map, layer))
                })
                .await?
        };

        Ok(layer)
    }

    /// Called from `freeze_inmem_layer`, returns true if successfully frozen.
    pub(crate) async fn try_freeze_in_memory_layer(
        &mut self,
        Lsn(last_record_lsn): Lsn,
        last_freeze_at: &AtomicLsn,
    ) {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );

        let end_lsn = Lsn(last_record_lsn + 1);

        if let Some(open_layer) = &self.snapshot.layer_map.open_layer {
            let open_layer_rc = Arc::clone(open_layer);
            // Does this layer need freezing?
            open_layer.freeze(end_lsn);

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            self.layer_manager
                .update(|mut layer_map| {
                    layer_map.frozen_layers.push_back(open_layer_rc);
                    layer_map.open_layer = None;
                    layer_map.next_open_layer_at = Some(end_lsn);
                    Ok((layer_map, ()))
                })
                .await
                .unwrap();

            last_freeze_at.store(end_lsn);
        }
    }

    /// Add image layers to the layer map, called from `create_image_layers`.
    pub(crate) async fn track_new_image_layers(&mut self, image_layers: Vec<ImageLayer>) {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );
        self.layer_manager
            .update(|mut layer_map| {
                let mut updates: BatchedUpdates<'_> = layer_map.batch_update();
                for layer in image_layers {
                    Self::insert_historic_layer(
                        Arc::new(layer),
                        &mut updates,
                        &self.snapshot.layer_fmgr,
                    );
                }
                updates.flush();
                Ok((layer_map, ()))
            })
            .await
            .unwrap();
    }

    /// Flush a frozen layer and add the written delta layer to the layer map.
    pub(crate) async fn finish_flush_l0_layer(
        &mut self,
        delta_layer: Option<DeltaLayer>,
        frozen_layer_for_check: &Arc<InMemoryLayer>,
    ) {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );
        self.layer_manager
            .update(|mut layer_map| {
                let l = layer_map.frozen_layers.pop_front();
                let mut updates = layer_map.batch_update();

                // Only one thread may call this function at a time (for this
                // timeline). If two threads tried to flush the same frozen
                // layer to disk at the same time, that would not work.
                assert!(compare_arced_layers(&l.unwrap(), frozen_layer_for_check));

                if let Some(delta_layer) = delta_layer {
                    Self::insert_historic_layer(
                        Arc::new(delta_layer),
                        &mut updates,
                        &self.snapshot.layer_fmgr,
                    );
                }
                updates.flush();
                Ok((layer_map, ()))
            })
            .await
            .unwrap();
    }

    /// Called when compaction is completed.
    pub(crate) async fn finish_compact_l0_consume_guard(
        self,
        _layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        compact_from: Vec<Arc<dyn PersistentLayer>>,
        compact_to: Vec<Arc<dyn PersistentLayer>>,
        metrics: &TimelineMetrics,
    ) -> Result<()> {
        assert!(
            self.pseudo_lock.is_right(),
            "should use `modify` guard for this function."
        );
        let compact_from = self
            .layer_manager
            .update(|mut layer_map| {
                let mut updates = layer_map.batch_update();
                for l in compact_to {
                    Self::insert_historic_layer(l, &mut updates, &self.snapshot.layer_fmgr);
                }
                for l in &compact_from {
                    // only remove from the layer map, not from file manager
                    updates.remove_historic(l.layer_desc().clone());
                }
                updates.flush();
                Ok((layer_map, Ok::<_, anyhow::Error>(compact_from)))
            })
            .await??;
        drop(self.pseudo_lock);
        // acquire the write lock so that all read threads are blocked, and once this lock is acquired,
        // new reads will be based on the updated layer map.
        let guard = self.layer_manager.pseudo_lock.write().await;
        drop(guard);
        // now that no one has access to the old layer map, we can safely remove the layers from disk.
        for layer in compact_from {
            self.snapshot.layer_fmgr.remove(layer.clone());
            // NB: the layer file identified by descriptor `l` is guaranteed to be present
            // in the LayerFileManager because compaction kept holding `layer_removal_cs` the entire
            // time, even though we dropped `Timeline::layers` inbetween.
            if !layer.is_remote_layer() {
                if let Err(e) = layer.delete_resident_layer_file() {
                    warn!("Failed to delete resident layer file: {}", e);
                } else {
                    let layer_file_size = layer.file_size();
                    metrics.resident_physical_size_gauge.sub(layer_file_size);
                }
            }
        }
        Ok(())
    }

    /// Called when garbage collect the timeline. Returns a guard that will apply the updates to the layer map.
    pub(crate) async fn finish_gc_timeline(
        &mut self,
        layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        gc_layers: Vec<Arc<dyn PersistentLayer>>,
        metrics: &TimelineMetrics,
    ) -> Result<()> {
        assert!(
            self.pseudo_lock.is_left(),
            "should use `write` guard for this function."
        );
        self.layer_manager
            .update(|mut layer_map| {
                let mut updates = layer_map.batch_update();
                for doomed_layer in gc_layers {
                    // TODO: decouple deletion and layer map modification
                    if let Err(e) = Self::delete_historic_layer(
                        layer_removal_cs.clone(),
                        doomed_layer,
                        &mut updates,
                        metrics,
                        &self.snapshot.layer_fmgr,
                    )
                    // FIXME: schedule succeeded deletions in timeline.rs `gc_timeline` instead of in batch?
                    {
                        updates.flush();
                        return Ok((layer_map, Err(e)));
                    }
                }
                updates.flush();
                Ok((layer_map, Ok(())))
            })
            .await
            .unwrap() // unwrap first level error
    }

    /// Helper function to insert a layer into the layer map and file manager.
    fn insert_historic_layer(
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        mapping: &LayerFileManager,
    ) {
        updates.insert_historic(layer.layer_desc().clone());
        mapping.insert(layer);
    }

    /// Helper function to remove a layer into the layer map and file manager
    fn remove_historic_layer(
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        mapping: &LayerFileManager,
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
        mapping: &LayerFileManager,
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
}

/// Manages the layer files in the local / remote file system. This is a wrapper around `DashMap`.
///
/// Developer notes: dashmap will deadlock in some cases. Please ensure only one reference to the element
/// in the dashmap is held in each of the functions.
pub struct LayerFileManager<T: AsLayerDesc + ?Sized = dyn PersistentLayer>(
    DashMap<PersistentLayerKey, Arc<T>>,
);

impl<T: AsLayerDesc + ?Sized> LayerFileManager<T> {
    pub(crate) fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<T> {
        // The assumption for the `expect()` is that all code maintains the following invariant:
        // A layer's descriptor is present in the LayerMap => the LayerFileManager contains a layer for the descriptor.
        self.0
            .get(&desc.key())
            .with_context(|| format!("get layer from desc: {}", desc.filename()))
            .expect("not found")
            .clone()
    }

    pub(crate) fn insert(&self, layer: Arc<T>) {
        let present = self.0.insert(layer.layer_desc().key(), layer.clone());
        if present.is_some() && cfg!(debug_assertions) {
            panic!("overwriting a layer: {:?}", layer.layer_desc())
        }
    }

    pub(crate) fn contains(&self, layer: &Arc<T>) -> bool {
        self.0.contains_key(&layer.layer_desc().key())
    }

    pub(crate) fn new() -> Self {
        Self(DashMap::new())
    }

    pub(crate) fn remove(&self, layer: Arc<T>) {
        let present = self.0.remove(&layer.layer_desc().key());
        if present.is_none() && cfg!(debug_assertions) {
            panic!(
                "removing layer that is not present in layer mapping: {:?}",
                layer.layer_desc()
            )
        }
    }

    pub(crate) fn replace_and_verify(&self, expected: Arc<T>, new: Arc<T>) -> Result<()> {
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

        if let Some(mut layer) = self.0.get_mut(&key) {
            anyhow::ensure!(
                compare_arced_layers(&expected, &*layer),
                "another layer was found instead of expected, expected={expected:?}, new={new:?}",
                expected = Arc::as_ptr(&expected),
                new = Arc::as_ptr(&*layer),
            );
            *layer = new;
            Ok(())
        } else {
            anyhow::bail!("layer was not found");
        }
    }
}
