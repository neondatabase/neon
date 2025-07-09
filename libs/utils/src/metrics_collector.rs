use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use metrics::{IntGauge, proto::MetricFamily, register_int_gauge};
use once_cell::sync::Lazy;

pub static METRICS_STALE_MILLIS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "metrics_metrics_stale_milliseconds",
        "The current metrics stale time in milliseconds"
    )
    .expect("failed to define a metric")
});

#[derive(Debug)]
pub struct CollectedMetrics {
    pub metrics: Vec<MetricFamily>,
    pub collected_at: Instant,
}

impl CollectedMetrics {
    fn new(metrics: Vec<MetricFamily>) -> Self {
        Self {
            metrics,
            collected_at: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct MetricsCollector {
    last_collected: RwLock<Arc<CollectedMetrics>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            last_collected: RwLock::new(Arc::new(CollectedMetrics::new(vec![]))),
        }
    }

    #[tracing::instrument(name = "metrics_collector", skip_all)]
    pub fn run_once(&self, cache_metrics: bool) -> Arc<CollectedMetrics> {
        let started = Instant::now();
        let metrics = metrics::gather();
        let collected = Arc::new(CollectedMetrics::new(metrics));
        if cache_metrics {
            let mut guard = self.last_collected.write().unwrap();
            *guard = collected.clone();
        }
        tracing::info!(
            "Collected {} metric families in {} ms",
            collected.metrics.len(),
            started.elapsed().as_millis()
        );
        collected
    }

    pub fn last_collected(&self) -> Arc<CollectedMetrics> {
        self.last_collected.read().unwrap().clone()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

// Interval for metrics collection. Currently hard-coded to be the same as the metrics scape interval from the obs agent
pub static METRICS_COLLECTION_INTERVAL: Duration = Duration::from_secs(30);

pub static METRICS_COLLECTOR: Lazy<MetricsCollector> = Lazy::new(MetricsCollector::default);
