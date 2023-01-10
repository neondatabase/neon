use super::layer_coverage::LayerCoverage;

/// Separate coverage data structure for each layer type.
///
/// This is just a tuple but with the elements named to
/// prevent bugs.
pub struct LatestLayerMap<Value> {
    pub image_coverage: LayerCoverage<Value>,
    pub delta_coverage: LayerCoverage<Value>,
}

impl<T: Clone> Default for LatestLayerMap<T> {
    fn default() -> Self {
        Self {
            image_coverage: LayerCoverage::default(),
            delta_coverage: LayerCoverage::default(),
        }
    }
}

impl<Value: Clone> LatestLayerMap<Value> {
    pub fn clone(self: &Self) -> Self {
        Self {
            image_coverage: self.image_coverage.clone(),
            delta_coverage: self.delta_coverage.clone(),
        }
    }
}
