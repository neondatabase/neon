use std::{collections::BTreeSet, ops::Range};

use utils::lsn::Lsn;

use super::Timeline;

#[derive(serde::Serialize)]
pub(crate) struct RangeAnalysis {
    start: String,
    end: String,
    has_image: bool,
    num_of_deltas_above_image: usize,
    total_num_of_deltas: usize,
    num_of_l0: usize,
}

impl Timeline {
    pub(crate) async fn perf_info(&self) -> Vec<RangeAnalysis> {
        // First, collect all split points of the layers.
        let mut split_points = BTreeSet::new();
        let mut delta_ranges = Vec::new();
        let mut image_ranges = Vec::new();

        let num_of_l0;
        let all_layer_files = {
            let guard = self.layers.read().await;
            num_of_l0 = guard.layer_map().unwrap().level0_deltas().len();
            guard.all_persistent_layers()
        };
        let lsn = self.get_last_record_lsn();

        for key in all_layer_files {
            split_points.insert(key.key_range.start);
            split_points.insert(key.key_range.end);
            if key.is_delta {
                delta_ranges.push((key.key_range.clone(), key.lsn_range.clone()));
            } else {
                image_ranges.push((key.key_range.clone(), key.lsn_range.start));
            }
        }

        // For each split range, compute the estimated read amplification.
        let split_points = split_points.into_iter().collect::<Vec<_>>();

        let mut result = Vec::new();

        for i in 0..(split_points.len() - 1) {
            let start = split_points[i];
            let end = split_points[i + 1];
            // Find the latest image layer that contains the information.
            let mut maybe_image_layers = image_ranges
                .iter()
                // We insert split points for all image layers, and therefore a `contains` check for the start point should be enough.
                .filter(|(key_range, img_lsn)| key_range.contains(&start) && img_lsn <= &lsn)
                .cloned()
                .collect::<Vec<_>>();
            maybe_image_layers.sort_by(|a, b| a.1.cmp(&b.1));
            let image_layer = maybe_image_layers.last().cloned();
            let lsn_filter_start = image_layer
                .as_ref()
                .map(|(_, lsn)| *lsn)
                .unwrap_or(Lsn::INVALID);

            fn overlaps_with(lsn_range_a: &Range<Lsn>, lsn_range_b: &Range<Lsn>) -> bool {
                !(lsn_range_a.end <= lsn_range_b.start || lsn_range_a.start >= lsn_range_b.end)
            }

            let maybe_delta_layers = delta_ranges
                .iter()
                .filter(|(key_range, lsn_range)| {
                    key_range.contains(&start) && overlaps_with(&(lsn_filter_start..lsn), lsn_range)
                })
                .cloned()
                .collect::<Vec<_>>();

            let pitr_delta_layers = delta_ranges
                .iter()
                .filter(|(key_range, _)| key_range.contains(&start))
                .cloned()
                .collect::<Vec<_>>();

            result.push(RangeAnalysis {
                start: start.to_string(),
                end: end.to_string(),
                has_image: image_layer.is_some(),
                num_of_deltas_above_image: maybe_delta_layers.len(),
                total_num_of_deltas: pitr_delta_layers.len(),
                num_of_l0,
            });
        }

        result
    }
}
