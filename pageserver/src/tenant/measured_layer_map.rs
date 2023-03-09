use std::sync::Arc;

use crate::metrics::NUM_ONDISK_LAYERS;
use crate::tenant::layer_map::{self, Replacement};
use crate::tenant::storage_layer::PersistentLayer;

/// Metrics updating wrapper around the real layermap.
///
/// The real layermap is needed to be generic over all layers to allow benchmarking and testing,
/// but for real pageserver use we need only `dyn PersistentLayer`.
///
/// `Deref` and `DerefMut` delegate to original layer map, this type only implements the needed
/// methods for metrics.
///
/// See [layer_map::LayerMap].
#[derive(Default)]
pub struct MeasuredLayerMap(layer_map::LayerMap<dyn PersistentLayer>);

impl std::ops::Deref for MeasuredLayerMap {
    type Target = layer_map::LayerMap<dyn PersistentLayer>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MeasuredLayerMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl MeasuredLayerMap {
    /// See [layer_map::LayerMap::batch_update].
    pub fn batch_update(&mut self) -> BatchedUpdates<'_> {
        BatchedUpdates {
            inner: self.0.batch_update(),
        }
    }
}

/// See [layer_map::BatchedUpdates].
///
/// Wrapper for handling metric updates.
#[must_use]
pub struct BatchedUpdates<'a> {
    inner: layer_map::BatchedUpdates<'a, dyn PersistentLayer>,
}

impl BatchedUpdates<'_> {
    /// See [layer_map::BatchedUpdates::insert_historic].
    pub fn insert_historic(&mut self, layer: Arc<dyn PersistentLayer>) {
        let is_remote = layer.is_remote_layer();

        self.inner.insert_historic(layer);

        if !is_remote {
            NUM_ONDISK_LAYERS.inc();
        }
    }

    /// See [layer_map::BatchedUpdates::remove_historic].
    pub fn remove_historic(&mut self, layer: Arc<dyn PersistentLayer>) {
        let is_remote = layer.is_remote_layer();

        self.inner.remove_historic(layer);

        if !is_remote {
            NUM_ONDISK_LAYERS.dec();
        }
    }

    /// See [layer_map::BatchedUpdates::replace_historic].
    pub fn replace_historic(
        &mut self,
        expected: &Arc<dyn PersistentLayer>,
        new: Arc<dyn PersistentLayer>,
    ) -> anyhow::Result<Replacement<Arc<dyn PersistentLayer>>> {
        use Replacement::*;

        let is_remote = new.is_remote_layer();

        let res = self.inner.replace_historic(expected, new)?;

        match &res {
            Replaced { .. } => {
                if is_remote != expected.is_remote_layer() {
                    if is_remote {
                        NUM_ONDISK_LAYERS.dec();
                    } else {
                        NUM_ONDISK_LAYERS.inc();
                    }
                }
            }
            NotFound | RemovalBuffered | Unexpected(_) => {}
        }

        Ok(res)
    }

    /// See [layer_map::BatchedUpdates::flush].
    pub fn flush(self) {
        self.inner.flush()
    }
}
