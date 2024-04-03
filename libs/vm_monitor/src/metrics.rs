use once_cell::sync::Lazy;
use prometheus::{opts, register_int_gauge, IntGauge};

pub(crate) static METRICS_LFC_HITS: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!(opts!("vm_monitor_lfc_hits", "",)).unwrap());

pub(crate) static METRICS_LFC_MISSES: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!(opts!("vm_monitor_lfc_misses", "",)).unwrap());

pub(crate) static METRICS_LFC_USED: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!(opts!("vm_monitor_lfc_used", "",)).unwrap());

pub(crate) static METRICS_LFC_WRITES: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!(opts!("vm_monitor_lfc_writes", "",)).unwrap());

pub(crate) static METRICS_LFC_WORKING_SET_SIZE: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge!(opts!("vm_monitor_lfc_working_set_size", "",)).unwrap());
