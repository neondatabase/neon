use super::remote_timeline_client::RemoteTimelineClient;
use super::storage_layer::{PersistentLayer, PersistentLayerDesc, PersistentLayerKey, RemoteLayer};
use super::Timeline;
use crate::tenant::layer_map::{self, LayerMap};
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use crate::tenant::storage_layer::LayerFileName;
use anyhow::{Context, Result};
use pageserver_api::models::TimelineState;
use remote_storage::GenericRemoteStorage;
use std::sync::{Mutex, Weak};
use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing::{error, info, warn};
use utils::id::{TenantId, TimelineId};

pub struct LayerCache {
    /// Layer removal lock.
    /// A lock to ensure that no layer of the timeline is removed concurrently by other tasks.
    /// This lock is acquired in [`Timeline::gc`], [`Timeline::compact`],
    /// and [`Tenant::delete_timeline`]. This is an `Arc<Mutex>` lock because we need an owned
    /// lock guard in functions that will be spawned to tokio I/O pool (which requires `'static`).
    pub layers_removal_lock: Arc<tokio::sync::Mutex<()>>,

    /// We need this lock b/c we do not have any way to prevent GC/compaction from removing files in-use.
    /// We need to do reference counting on Arc to prevent this from happening, and we can safely remove this lock.
    pub layers_operation_lock: Arc<tokio::sync::RwLock<()>>,

    /// Will be useful when we move evict / download to layer cache.
    #[allow(unused)]
    timeline: Weak<Timeline>,

    mapping: Mutex<HashMap<PersistentLayerKey, Arc<dyn PersistentLayer>>>,

    tenant_id: TenantId,
    timeline_id: TimelineId,
}

pub struct LayerInUseWrite(tokio::sync::OwnedRwLockWriteGuard<()>);

pub struct LayerInUseRead(tokio::sync::OwnedRwLockReadGuard<()>);

#[derive(Clone)]
pub struct DeleteGuard(Arc<tokio::sync::OwnedMutexGuard<()>>);

impl LayerCache {
    pub fn new(timeline: Weak<Timeline>) -> Self {
        let timeline_owned = timeline.upgrade().expect("cannot upgrade");
        Self {
            layers_operation_lock: Arc::new(tokio::sync::RwLock::new(())),
            layers_removal_lock: Arc::new(tokio::sync::Mutex::new(())),
            mapping: Mutex::new(HashMap::new()),
            timeline,
            tenant_id: timeline_owned.tenant_id,
            timeline_id: timeline_owned.timeline_id,
        }
    }

    pub fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        let guard = self.mapping.lock().unwrap();
        guard.get(&desc.key()).expect("not found").clone()
    }

    /// This function is to mock the original behavior of `layers` lock in `Timeline`. Can be removed after we ensure
    /// we won't delete files that are being read.
    pub async fn layer_in_use_write(&self) -> LayerInUseWrite {
        LayerInUseWrite(self.layers_operation_lock.clone().write_owned().await)
    }

    /// This function is to mock the original behavior of `layers` lock in `Timeline`. Can be removed after we ensure
    /// we won't delete files that are being read.
    pub async fn layer_in_use_read(&self) -> LayerInUseRead {
        LayerInUseRead(self.layers_operation_lock.clone().read_owned().await)
    }

    /// Ensures only one of compaction / gc can happen at a time.
    pub async fn delete_guard(&self) -> DeleteGuard {
        DeleteGuard(Arc::new(
            self.layers_removal_lock.clone().lock_owned().await,
        ))
    }

    /// Should only be called when initializing the timeline. Bypass checks and layer operation lock.
    pub fn remove_local_when_init(&self, layer: Arc<dyn PersistentLayer>) {
        let mut guard = self.mapping.lock().unwrap();
        guard.remove(&layer.layer_desc().key());
    }

    /// Should only be called when initializing the timeline. Bypass checks and layer operation lock.
    pub fn populate_remote_when_init(&self, layer: Arc<RemoteLayer>) {
        let mut guard = self.mapping.lock().unwrap();
        guard.insert(layer.layer_desc().key(), layer);
    }

    /// Should only be called when initializing the timeline. Bypass checks and layer operation lock.
    pub fn populate_local_when_init(&self, layer: Arc<dyn PersistentLayer>) {
        let mut guard = self.mapping.lock().unwrap();
        guard.insert(layer.layer_desc().key(), layer);
    }

    /// Called within read path.
    pub fn replace_and_verify(
        &self,
        expected: Arc<dyn PersistentLayer>,
        new: Arc<dyn PersistentLayer>,
    ) -> Result<()> {
        let mut guard = self.mapping.lock().unwrap();

        use super::layer_map::LayerKey;
        let key = LayerKey::from(&*expected);
        let other = LayerKey::from(&*new);

        let expected_l0 = LayerMap::is_l0(expected.layer_desc());
        let new_l0 = LayerMap::is_l0(new.layer_desc());

        fail::fail_point!("layermap-replace-notfound", |_| anyhow::bail!(
            "replacing downloaded layer into layermap failed because layer was not found"
        ));

        anyhow::ensure!(
            key == other,
            "replacing downloaded layer into layermap failed because two layers have different keys: {key:?} != {other:?}"
        );

        anyhow::ensure!(
             expected_l0 == new_l0,
             "replacing downloaded layer into layermap failed because one layer is l0 while the other is not: {expected_l0} != {new_l0}"
         );

        if let Some(layer) = guard.get_mut(&expected.layer_desc().key()) {
            anyhow::ensure!(
                layer_map::compare_arced_layers(&expected, layer),
                "replacing downloaded layer into layermap failed because another layer was found instead of expected, expected={expected:?}, new={new:?}",
                expected = Arc::as_ptr(&expected),
                new = Arc::as_ptr(layer),
            );
            *layer = new;
            Ok(())
        } else {
            anyhow::bail!(
                "replacing downloaded layer into layermap failed because layer was not found"
            );
        }
    }

    /// Called within write path. When compaction and image layer creation we will create new layers.
    pub fn create_new_layer(&self, layer: Arc<dyn PersistentLayer>) {
        let mut guard = self.mapping.lock().unwrap();
        guard.insert(layer.layer_desc().key(), layer);
    }

    /// Called within write path. When GC and compaction we will remove layers and delete them on disk.
    /// Will move logic to delete files here later.
    pub fn delete_layer(&self, layer: Arc<dyn PersistentLayer>) {
        let mut guard = self.mapping.lock().unwrap();
        guard.remove(&layer.layer_desc().key());
    }

    #[instrument(skip_all, fields(tenant = %self.tenant_id, timeline = %self.timeline_id))]
    pub async fn download_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
        let Some(layer) = self.find_layer(layer_file_name) else { return Ok(None) };
        let Some(remote_layer) = layer.downcast_remote_layer() else { return  Ok(Some(false)) };
        let Some(timeline) = self.timeline.upgrade() else { return Ok(None) };
        if timeline.remote_client.is_none() {
            return Ok(Some(false));
        }

        timeline.download_remote_layer(remote_layer).await?;
        Ok(Some(true))
    }

    fn find_layer(&self, layer_file_name: &str) -> Option<Arc<dyn PersistentLayer>> {
        let layers = self.mapping.lock().unwrap();
        for (_, historic_layer) in layers.iter() {
            let historic_layer_name = historic_layer.filename().file_name();
            if layer_file_name == historic_layer_name {
                return Some(historic_layer.clone());
            }
        }

        None
    }

    /// Like [`evict_layer_batch`], but for just one layer.
    /// Additional case `Ok(None)` covers the case where the layer could not be found by its `layer_file_name`.
    pub async fn evict_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
        let Some(local_layer) = self.find_layer(layer_file_name) else { return Ok(None) };
        let Some(timeline) = self.timeline.upgrade() else { return Ok(None) };
        let remote_client = timeline
            .remote_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("remote storage not configured; cannot evict"))?;

        let cancel = CancellationToken::new();
        let results = self
            .evict_layer_batch(remote_client, &[local_layer], cancel)
            .await?;
        assert_eq!(results.len(), 1);
        let result: Option<anyhow::Result<bool>> = results.into_iter().next().unwrap();
        match result {
            None => anyhow::bail!("task_mgr shutdown requested"),
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => Err(e),
        }
    }

    /// Evict a batch of layers.
    ///
    /// GenericRemoteStorage reference is required as a witness[^witness_article] for "remote storage is configured."
    ///
    /// [^witness_article]: https://willcrichton.net/rust-api-type-patterns/witnesses.html
    pub async fn evict_layers(
        &self,
        _: &GenericRemoteStorage,
        layers_to_evict: &[Arc<dyn PersistentLayer>],
        cancel: CancellationToken,
    ) -> anyhow::Result<Vec<Option<anyhow::Result<bool>>>> {
        let Some(timeline) = self.timeline.upgrade() else { return Ok(vec![]) };
        let remote_client = timeline.remote_client.clone().expect(
            "GenericRemoteStorage is configured, so timeline must have RemoteTimelineClient",
        );

        self.evict_layer_batch(&remote_client, layers_to_evict, cancel)
            .await
    }

    /// Evict multiple layers at once, continuing through errors.
    ///
    /// Try to evict the given `layers_to_evict` by
    ///
    /// 1. Replacing the given layer object in the layer map with a corresponding [`RemoteLayer`] object.
    /// 2. Deleting the now unreferenced layer file from disk.
    ///
    /// The `remote_client` should be this timeline's `self.remote_client`.
    /// We make the caller provide it so that they are responsible for handling the case
    /// where someone wants to evict the layer but no remote storage is configured.
    ///
    /// Returns either `Err()` or `Ok(results)` where `results.len() == layers_to_evict.len()`.
    /// If `Err()` is returned, no eviction was attempted.
    /// Each position of `Ok(results)` corresponds to the layer in `layers_to_evict`.
    /// Meaning of each `result[i]`:
    /// - `Some(Err(...))` if layer replacement failed for an unexpected reason
    /// - `Some(Ok(true))` if everything went well.
    /// - `Some(Ok(false))` if there was an expected reason why the layer could not be replaced, e.g.:
    ///    - evictee was not yet downloaded
    ///    - replacement failed for an expectable reason (e.g., layer removed by GC before we grabbed all locks)
    /// - `None` if no eviction attempt was made for the layer because `cancel.is_cancelled() == true`.
    async fn evict_layer_batch(
        &self,
        remote_client: &Arc<RemoteTimelineClient>,
        layers_to_evict: &[Arc<dyn PersistentLayer>],
        cancel: CancellationToken,
    ) -> anyhow::Result<Vec<Option<anyhow::Result<bool>>>> {
        // ensure that the layers have finished uploading
        // (don't hold the layer_removal_cs while we do it, we're not removing anything yet)
        remote_client
            .wait_completion()
            .await
            .context("wait for layer upload ops to complete")?;

        // now lock out layer removal (compaction, gc, timeline deletion)
        let layer_removal_guard = self.delete_guard().await;

        {
            // to avoid racing with detach and delete_timeline
            let Some(timeline) = self.timeline.upgrade() else { return Ok(vec![]) };
            let state = timeline.current_state();
            anyhow::ensure!(
                state == TimelineState::Active,
                "timeline is not active but {state:?}"
            );
        }

        let mut results = Vec::with_capacity(layers_to_evict.len());

        for l in layers_to_evict.iter() {
            let res = if cancel.is_cancelled() {
                None
            } else {
                Some(self.evict_layer_batch_impl(&layer_removal_guard, l))
            };
            results.push(res);
        }

        // commit the updates & release locks
        drop(layer_removal_guard);

        assert_eq!(results.len(), layers_to_evict.len());
        Ok(results)
    }

    fn evict_layer_batch_impl(
        &self,
        _layer_removal_cs: &DeleteGuard,
        local_layer: &Arc<dyn PersistentLayer>,
    ) -> anyhow::Result<bool> {
        if local_layer.is_remote_layer() {
            // TODO(issue #3851): consider returning an err here instead of false,
            // which is the same out the match later
            return Ok(false);
        }

        let layer_file_size = local_layer.file_size();

        let local_layer_mtime = local_layer
            .local_path()
            .expect("local layer should have a local path")
            .metadata()
            .context("get local layer file stat")?
            .modified()
            .context("get mtime of layer file")?;
        let local_layer_residence_duration =
            match SystemTime::now().duration_since(local_layer_mtime) {
                Err(e) => {
                    warn!("layer mtime is in the future: {}", e);
                    None
                }
                Ok(delta) => Some(delta),
            };

        let layer_metadata = LayerFileMetadata::new(layer_file_size);

        let new_remote_layer = Arc::new(match local_layer.filename() {
            LayerFileName::Image(image_name) => RemoteLayer::new_img(
                self.tenant_id,
                self.timeline_id,
                &image_name,
                &layer_metadata,
                local_layer
                    .access_stats()
                    .clone_for_residence_change(LayerResidenceStatus::Evicted),
            ),
            LayerFileName::Delta(delta_name) => RemoteLayer::new_delta(
                self.tenant_id,
                self.timeline_id,
                &delta_name,
                &layer_metadata,
                local_layer
                    .access_stats()
                    .clone_for_residence_change(LayerResidenceStatus::Evicted),
            ),
        });

        assert_eq!(local_layer.layer_desc(), new_remote_layer.layer_desc());

        let succeed = match self.replace_and_verify(local_layer.clone(), new_remote_layer) {
            Ok(()) => {
                if let Err(e) = local_layer.delete_resident_layer_file() {
                    error!("failed to remove layer file on evict after replacement: {e:#?}");
                }
                // Always decrement the physical size gauge, even if we failed to delete the file.
                // Rationale: we already replaced the layer with a remote layer in the layer map,
                // and any subsequent download_remote_layer will
                // 1. overwrite the file on disk and
                // 2. add the downloaded size to the resident size gauge.
                //
                // If there is no re-download, and we restart the pageserver, then load_layer_map
                // will treat the file as a local layer again, count it towards resident size,
                // and it'll be like the layer removal never happened.
                // The bump in resident size is perhaps unexpected but overall a robust behavior.

                let Some(timeline) = self.timeline.upgrade() else { return Ok(false) };

                timeline
                    .metrics
                    .resident_physical_size_gauge
                    .sub(layer_file_size);

                timeline.metrics.evictions.inc();

                if let Some(delta) = local_layer_residence_duration {
                    timeline
                        .metrics
                        .evictions_with_low_residence_duration
                        .read()
                        .unwrap()
                        .observe(delta);
                    info!(layer=%local_layer.short_id(), residence_millis=delta.as_millis(), "evicted layer after known residence period");
                } else {
                    info!(layer=%local_layer.short_id(), "evicted layer after unknown residence period");
                }

                true
            }
            Err(err) => {
                if cfg!(debug_assertions) {
                    error!(evicted=?local_layer, "failed to replace: {err}");
                } else {
                    error!(evicted=?local_layer, "failed to replace: {err}");
                }
                false
            }
        };

        Ok(succeed)
    }
}
