use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::storage_layer::{LayerFileName, PersistentLayer, RemoteLayerDesc};

pub struct LayerCache {
    layers: Mutex<HashMap<LayerFileName, Arc<dyn PersistentLayer>>>,
}

impl LayerCache {
    pub fn new() -> Self {
        Self {
            layers: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, layer_fname: &LayerFileName) -> Option<Arc<dyn PersistentLayer>> {
        let guard: std::sync::MutexGuard<HashMap<LayerFileName, Arc<dyn PersistentLayer>>> =
            self.layers.lock().unwrap();
        guard.get(layer_fname).cloned()
    }

    pub fn contains(&self, layer_fname: &LayerFileName) -> bool {
        let guard = self.layers.lock().unwrap();
        guard.contains_key(layer_fname)
    }

    pub fn insert(&self, layer_fname: LayerFileName, persistent_layer: Arc<dyn PersistentLayer>) {
        let mut guard = self.layers.lock().unwrap();
        guard.insert(layer_fname, persistent_layer);
    }
}
