//! This module implements `LayerMapMgr`, which manages a layer map object and provides lock-free access to the state.
//!
//! A common usage pattern is as follows:
//!
//! ```ignore
//! async fn compaction(&self) {
//!     // Get the current state.
//!     let state = self.layer_map_mgr.read();
//!     // No lock held at this point. Do compaction based on the state. This part usually incurs I/O operations and may
//!     // take a long time.
//!     let compaction_result = self.do_compaction(&state).await?;
//!     // Update the state.
//!     self.layer_map_mgr.update(|mut state| async move {
//!         // do updates to the state, return it.
//!         Ok(state)
//!     }).await?;
//! }
//! ```
use anyhow::Result;
use arc_swap::ArcSwap;
use futures::Future;
use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use super::layer_map::LayerMap;

/// Manages the storage state. Provide utility functions to modify the layer map and get an immutable reference to the
/// layer map.
pub struct LayerMapMgr {
    layer_map: ArcSwap<LayerMapGuard>,
    state_lock: tokio::sync::Mutex<()>,
    watermark: Arc<LayerMgrWatermark>,
}

/// A guard that holds a version of the layer map. When dropped, the version is released and the watermark will be updated.
#[derive(Clone)]
pub struct LayerMapGuard {
    version: u64,
    layer_map: LayerMap,
    watermark: Arc<LayerMgrWatermark>,
}

impl std::ops::Deref for LayerMapGuard {
    type Target = LayerMap;

    fn deref(&self) -> &Self::Target {
        &self.layer_map
    }
}

impl Drop for LayerMapGuard {
    fn drop(&mut self) {
        self.watermark.release(self.version);
    }
}

impl LayerMapMgr {
    /// Get the current state of the layer map.
    pub fn read(&self) -> Arc<LayerMapGuard> {
        // TODO: it is possible to use `load` to reduce the overhead of cloning the Arc, but read path usually involves
        // disk reads and layer mapping fetching, and therefore it's not a big deal to use a more optimized version
        // here.
        self.layer_map.load_full()
    }

    /// Clone the layer map for modification.
    fn clone_for_write(
        &self,
        _state_lock_witness: &tokio::sync::MutexGuard<'_, ()>,
    ) -> LayerMapGuard {
        (**self.layer_map.load()).clone()
    }

    pub fn new(layer_map: LayerMap) -> Self {
        const INITIAL_VERSION: u64 = 0;
        let watermark = Arc::new(LayerMgrWatermark::new(INITIAL_VERSION));
        Self {
            layer_map: ArcSwap::new(Arc::new(LayerMapGuard {
                version: INITIAL_VERSION,
                layer_map,
                watermark: watermark.clone(),
            })),
            watermark,
            state_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Update the layer map.
    pub async fn update<O, F>(&self, operation: O) -> Result<()>
    where
        O: FnOnce(LayerMap) -> F,
        F: Future<Output = Result<LayerMap>>,
    {
        let state_lock = self.state_lock.lock().await;
        let mut guard = self.clone_for_write(&state_lock);
        guard.version += 1;
        let layer_map = std::mem::take(&mut guard.layer_map);
        guard.layer_map = operation(layer_map).await?;
        self.layer_map.store(Arc::new(guard));
        Ok(())
    }

    pub fn lowest_version_in_use(&self) -> u64 {
        self.watermark.lowest_version_in_use()
    }
}

struct LayerMgrWatermarkCore {
    lowest_version_in_use: u64,
    versions_in_use: BinaryHeap<Reverse<u64>>,
}

/// Computes the lowest version used by any read thread. Once a version is not used any more,
/// we can remove all layers that are intended to be removed in that version.
struct LayerMgrWatermark {
    core: std::sync::Mutex<LayerMgrWatermarkCore>,
}

impl LayerMgrWatermark {
    fn new(initial_version: u64) -> Self {
        Self {
            core: std::sync::Mutex::new(LayerMgrWatermarkCore {
                lowest_version_in_use: initial_version,
                versions_in_use: BinaryHeap::new(),
            }),
        }
    }

    fn lowest_version_in_use(&self) -> u64 {
        self.core.lock().unwrap().lowest_version_in_use
    }

    fn release(&self, version: u64) {
        let mut core = self.core.lock().unwrap();
        match version.cmp(&core.lowest_version_in_use) {
            std::cmp::Ordering::Less => {
                if cfg!(debug_assertions) {
                    // TODO(chi): this panic might not be correctly handled by the panic handler
                    // given this function is called in a drop handler. We can move it to a separate
                    // thread if necessary.
                    panic!("release a version lower than the lowest version in use.")
                }
            }
            std::cmp::Ordering::Equal => {
                // Find the next version in use.
                let mut current_version = version + 1;
                while let Some(Reverse(next_version)) = core.versions_in_use.peek() {
                    if *next_version == current_version {
                        current_version += 1;
                        core.versions_in_use.pop();
                    } else {
                        break;
                    }
                }
                core.lowest_version_in_use = current_version;
            }
            std::cmp::Ordering::Greater => {
                // This version is in use. Add it to the heap.
                core.versions_in_use.push(Reverse(version));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use utils::{
        id::{TenantId, TimelineId},
        lsn::Lsn,
    };

    use crate::{repository::Key, tenant::storage_layer::PersistentLayerDesc};

    use super::*;

    #[tokio::test]
    async fn test_layer_map_manage() -> Result<()> {
        let mgr = LayerMapMgr::new(Default::default());
        mgr.update(|mut map| async move {
            let mut updates = map.batch_update();
            updates.insert_historic(PersistentLayerDesc::new_img(
                TenantId::generate(),
                TimelineId::generate(),
                Key::from_i128(0)..Key::from_i128(1),
                Lsn(0),
                false,
                0,
            ));
            updates.flush();
            Ok(map)
        })
        .await?;

        let ref_1 = mgr.read();

        mgr.update(|mut map| async move {
            let mut updates = map.batch_update();
            updates.insert_historic(PersistentLayerDesc::new_img(
                TenantId::generate(),
                TimelineId::generate(),
                Key::from_i128(1)..Key::from_i128(2),
                Lsn(0),
                false,
                0,
            ));
            updates.flush();
            Ok(map)
        })
        .await?;

        let ref_2 = mgr.read();

        // Modification should not be visible to the old reference.
        assert_eq!(
            ref_1
                .search(Key::from_i128(0), Lsn(1))
                .unwrap()
                .layer
                .key_range,
            Key::from_i128(0)..Key::from_i128(1)
        );
        assert!(ref_1.search(Key::from_i128(1), Lsn(1)).is_none());

        // Modification should be visible to the new reference.
        assert_eq!(
            ref_2
                .search(Key::from_i128(0), Lsn(1))
                .unwrap()
                .layer
                .key_range,
            Key::from_i128(0)..Key::from_i128(1)
        );
        assert_eq!(
            ref_2
                .search(Key::from_i128(1), Lsn(1))
                .unwrap()
                .layer
                .key_range,
            Key::from_i128(1)..Key::from_i128(2)
        );

        assert_eq!(mgr.lowest_version_in_use(), 1);
        drop(ref_1);
        drop(ref_2);
        assert_eq!(mgr.lowest_version_in_use(), 2);
        mgr.update(|map| async move { Ok(map) }).await?;
        assert_eq!(mgr.lowest_version_in_use(), 3);

        Ok(())
    }

    #[test]
    fn test_watermark() {
        let watermark = LayerMgrWatermark::new(0);
        assert_eq!(watermark.lowest_version_in_use(), 0);
        watermark.release(0);
        assert_eq!(watermark.lowest_version_in_use(), 1);
        watermark.release(1);
        assert_eq!(watermark.lowest_version_in_use(), 2);
        watermark.release(3);
        watermark.release(4);
        watermark.release(5);
        watermark.release(7);
        assert_eq!(watermark.lowest_version_in_use(), 2);
        watermark.release(2);
        assert_eq!(watermark.lowest_version_in_use(), 6);
        watermark.release(6);
        assert_eq!(watermark.lowest_version_in_use(), 8);
    }
}
