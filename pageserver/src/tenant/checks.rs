use std::collections::BTreeSet;

use itertools::Itertools;

use super::storage_layer::LayerName;

/// Checks whether a layer map is valid (i.e., is a valid result of the current compaction algorithm if nothing goes wrong).
/// The function checks if we can split the LSN range of a delta layer only at the LSNs of the delta layers. For example,
///
/// ```plain
/// |       |                 |       |
/// |   1   |    |   2   |    |   3   |
/// |       |    |       |    |       |
/// ```
///
/// This is not a valid layer map because the LSN range of layer 1 intersects with the LSN range of layer 2. 1 and 2 should have
/// the same LSN range.
///
/// The exception is that when layer 2 only contains a single key, it could be split over the LSN range. For example,
///
/// ```plain
/// |       |    |   2   |    |       |
/// |   1   |    |-------|    |   3   |
/// |       |    |   4   |    |       |
///
/// If layer 2 and 4 contain the same single key, this is also a valid layer map.
pub fn check_valid_layermap(metadata: &[LayerName]) -> Option<String> {
    let mut lsn_split_point = BTreeSet::new(); // TODO: use a better data structure (range tree / range set?)
    let mut all_delta_layers = Vec::new();
    for name in metadata {
        if let LayerName::Delta(layer) = name {
            if layer.key_range.start.next() != layer.key_range.end {
                all_delta_layers.push(layer.clone());
            }
        }
    }
    for layer in &all_delta_layers {
        let lsn_range = &layer.lsn_range;
        lsn_split_point.insert(lsn_range.start);
        lsn_split_point.insert(lsn_range.end);
    }
    for layer in &all_delta_layers {
        let lsn_range = layer.lsn_range.clone();
        let intersects = lsn_split_point.range(lsn_range).collect_vec();
        if intersects.len() > 1 {
            let err = format!(
                        "layer violates the layer map LSN split assumption: layer {} intersects with LSN [{}]",
                        layer,
                        intersects.into_iter().map(|lsn| lsn.to_string()).join(", ")
                    );
            return Some(err);
        }
    }
    None
}
