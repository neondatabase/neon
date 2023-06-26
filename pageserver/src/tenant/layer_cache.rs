use super::storage_layer::{PersistentLayer, PersistentLayerDesc, PersistentLayerKey, RemoteLayer};
use super::Timeline;
use crate::metrics::{STORAGE_PHYSICAL_SIZE, STORAGE_PHYSICAL_SIZE_FILE_TYPE};
use crate::tenant::layer_map::{self, LayerMap};
use anyhow::Result;
use std::sync::{Mutex, Weak};
use std::{collections::HashMap, sync::Arc};
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

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub tenant_id_str: String,
    pub timeline_id_str: String,

    mapping: Mutex<HashMap<PersistentLayerKey, Arc<dyn PersistentLayer>>>,
}

pub struct LayerInUseWrite(tokio::sync::OwnedRwLockWriteGuard<()>);

pub struct LayerInUseRead(tokio::sync::OwnedRwLockReadGuard<()>);

#[derive(Clone)]
pub struct DeleteGuard(Arc<tokio::sync::OwnedMutexGuard<()>>);

impl LayerCache {
    pub fn new(timeline: Weak<Timeline>) -> Self {
        let timeline_arc = timeline.upgrade().unwrap();
        Self {
            layers_operation_lock: Arc::new(tokio::sync::RwLock::new(())),
            layers_removal_lock: Arc::new(tokio::sync::Mutex::new(())),
            mapping: Mutex::new(HashMap::new()),
            timeline,
            tenant_id: timeline_arc.tenant_id,
            timeline_id: timeline_arc.timeline_id,
            tenant_id_str: timeline_arc.tenant_id.to_string(),
            timeline_id_str: timeline_arc.timeline_id.to_string(),
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
        self.metrics_size_sub(&*layer);
        let mut guard = self.mapping.lock().unwrap();
        guard.remove(&layer.layer_desc().key());
    }

    /// Should only be called when initializing the timeline. Bypass checks and layer operation lock.
    pub fn populate_remote_when_init(&self, layer: Arc<RemoteLayer>) {
        self.metrics_size_add(&*layer);
        let mut guard = self.mapping.lock().unwrap();
        guard.insert(layer.layer_desc().key(), layer);
    }

    /// Should only be called when initializing the timeline. Bypass checks and layer operation lock.
    pub fn populate_local_when_init(&self, layer: Arc<dyn PersistentLayer>) {
        self.metrics_size_add(&*layer);
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
        self.metrics_size_add(&*layer);
        let mut guard = self.mapping.lock().unwrap();
        guard.insert(layer.layer_desc().key(), layer);
    }

    /// Called within write path. When GC and compaction we will remove layers and delete them on disk.
    /// Will move logic to delete files here later.
    pub fn delete_layer(&self, layer: Arc<dyn PersistentLayer>) {
        self.metrics_size_sub(&*layer);
        let mut guard = self.mapping.lock().unwrap();
        guard.remove(&layer.layer_desc().key());
    }

    fn metrics_size_add(&self, layer: &dyn PersistentLayer) {
        STORAGE_PHYSICAL_SIZE
            .with_label_values(&[
                Self::get_layer_type(layer),
                &self.tenant_id_str,
                &self.timeline_id_str,
            ])
            .add(layer.file_size() as i64);
    }

    fn metrics_size_sub(&self, layer: &dyn PersistentLayer) {
        STORAGE_PHYSICAL_SIZE
            .with_label_values(&[
                Self::get_layer_type(layer),
                &self.tenant_id_str,
                &self.timeline_id_str,
            ])
            .sub(layer.file_size() as i64);
    }

    fn get_layer_type(layer: &dyn PersistentLayer) -> &'static str {
        if layer.is_delta() {
            &STORAGE_PHYSICAL_SIZE_FILE_TYPE[1]
        } else if layer.is_incremental() {
            &STORAGE_PHYSICAL_SIZE_FILE_TYPE[2]
        } else {
            &STORAGE_PHYSICAL_SIZE_FILE_TYPE[0]
        }
    }
}
