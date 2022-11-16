//! Broker metrics.

use metrics::{register_int_gauge, IntGauge};
use once_cell::sync::Lazy;

pub static NUM_PUBS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("storage_broker_active_publishers", "Number of publications")
        .expect("Failed to register metric")
});

pub static NUM_SUBS_TIMELINE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "storage_broker_per_timeline_active_subscribers",
        "Number of subsciptions to particular tenant timeline id"
    )
    .expect("Failed to register metric")
});

pub static NUM_SUBS_ALL: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "storage_broker_all_keys_active_subscribers",
        "Number of subsciptions to all keys"
    )
    .expect("Failed to register metric")
});
