//! Broker metrics.

use metrics::{register_int_counter, register_int_gauge, IntCounter, IntGauge};
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

pub static PROCESSED_MESSAGES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "storage_broker_processed_messages_total",
        "Number of messages received by storage broker, before routing and broadcasting"
    )
    .expect("Failed to register metric")
});

pub static BROADCASTED_MESSAGES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "storage_broker_broadcasted_messages_total",
        "Number of messages broadcasted (sent over network) to subscribers"
    )
    .expect("Failed to register metric")
});

pub static BROADCAST_DROPPED_MESSAGES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "storage_broker_broadcast_dropped_messages_total",
        "Number of messages dropped due to channel capacity overflow"
    )
    .expect("Failed to register metric")
});

pub static PUBLISHED_ONEOFF_MESSAGES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "storage_broker_published_oneoff_messages_total",
        "Number of one-off messages sent via PublishOne method"
    )
    .expect("Failed to register metric")
});
