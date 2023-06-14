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
use std::sync::Arc;

use super::layer_map::LayerMap;

/// Manages the storage state. Provide utility functions to modify the layer map and get an immutable reference to the
/// layer map.
pub struct LayerMapMgr {
    layer_map: ArcSwap<LayerMap>,
    state_lock: tokio::sync::Mutex<()>,
}

impl LayerMapMgr {
    /// Get the current state of the layer map.
    pub fn read(&self) -> Arc<LayerMap> {
        // TODO: it is possible to use `load` to reduce the overhead of cloning the Arc, but read path usually involves
        // disk reads and layer mapping fetching, and therefore it's not a big deal to use a more optimized version
        // here.
        self.layer_map.load_full()
    }

    /// Clone the layer map for modification.
    fn clone_for_write(&self, _state_lock_witness: &tokio::sync::MutexGuard<'_, ()>) -> LayerMap {
        (**self.layer_map.load()).clone()
    }

    pub fn new(layer_map: LayerMap) -> Self {
        Self {
            layer_map: ArcSwap::new(Arc::new(layer_map)),
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
        let state = self.clone_for_write(&state_lock);
        let new_state = operation(state).await?;
        self.layer_map.store(Arc::new(new_state));
        Ok(())
    }

    /// Update the layer map.
    pub  fn update_sync<O>(&self, operation: O) -> Result<()>
    where
        O: FnOnce(LayerMap) -> Result<LayerMap>,
    {
        let state_lock = self.state_lock.blocking_lock();
        let state = self.clone_for_write(&state_lock);
        let new_state = operation(state)?;
        self.layer_map.store(Arc::new(new_state));
        Ok(())
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
        Ok(())
    }
}
