use anyhow::Result;
use serde::Serialize;
use std::ops::AddAssign;
use std::time::Duration;

///
/// Result of performing GC
///
#[derive(Default, Serialize, Debug)]
pub struct GcResult {
    pub layers_total: u64,
    pub layers_needed_by_cutoff: u64,
    pub layers_needed_by_pitr: u64,
    pub layers_needed_by_branches: u64,
    pub layers_needed_by_leases: u64,
    pub layers_not_updated: u64,
    pub layers_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.

    #[serde(serialize_with = "serialize_duration_as_millis")]
    pub elapsed: Duration,

    /// The layers which were garbage collected.
    ///
    /// Used in `/v1/tenant/:tenant_id/timeline/:timeline_id/do_gc` to wait for the layers to be
    /// dropped in tests.
    #[cfg(feature = "testing")]
    #[serde(skip)]
    pub(crate) doomed_layers: Vec<crate::tenant::storage_layer::Layer>,
}

// helper function for `GcResult`, serializing a `Duration` as an integer number of milliseconds
fn serialize_duration_as_millis<S>(d: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    d.as_millis().serialize(serializer)
}

impl AddAssign for GcResult {
    fn add_assign(&mut self, other: Self) {
        self.layers_total += other.layers_total;
        self.layers_needed_by_pitr += other.layers_needed_by_pitr;
        self.layers_needed_by_cutoff += other.layers_needed_by_cutoff;
        self.layers_needed_by_branches += other.layers_needed_by_branches;
        self.layers_needed_by_leases += other.layers_needed_by_leases;
        self.layers_not_updated += other.layers_not_updated;
        self.layers_removed += other.layers_removed;

        self.elapsed += other.elapsed;

        #[cfg(feature = "testing")]
        {
            let mut other = other;
            self.doomed_layers.append(&mut other.doomed_layers);
        }
    }
}
