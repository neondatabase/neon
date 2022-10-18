use metrics::core::{AtomicU64, GenericCounter};
use metrics::{
    register_gauge_vec, register_histogram, register_histogram_vec, register_int_counter,
    register_int_counter_vec, register_int_gauge, register_int_gauge_vec, register_uint_gauge_vec,
    GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, UIntGauge,
    UIntGaugeVec,
};
use once_cell::sync::Lazy;
use utils::id::{TenantId, TimelineId};

/// Prometheus histogram buckets (in seconds) that capture the majority of
/// latencies in the microsecond range but also extend far enough up to distinguish
/// "bad" from "really bad".
fn get_buckets_for_critical_operations() -> Vec<f64> {
    let buckets_per_digit = 5;
    let min_exponent = -6;
    let max_exponent = 2;

    let mut buckets = vec![];
    // Compute 10^(exp / buckets_per_digit) instead of 10^(1/buckets_per_digit)^exp
    // because it's more numerically stable and doesn't result in numbers like 9.999999
    for exp in (min_exponent * buckets_per_digit)..=(max_exponent * buckets_per_digit) {
        buckets.push(10_f64.powf(exp as f64 / buckets_per_digit as f64))
    }
    buckets
}

// Metrics collected on operations on the storage repository.
const STORAGE_TIME_OPERATIONS: &[&str] = &[
    "layer flush",
    "compact",
    "create images",
    "init logical size",
    "load layer map",
    "gc",
];

pub static STORAGE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_storage_operations_seconds",
        "Time spent on storage operations",
        &["operation", "tenant_id", "timeline_id"],
        get_buckets_for_critical_operations(),
    )
    .expect("failed to define a metric")
});

// Metrics collected on operations on the storage repository.
static RECONSTRUCT_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_getpage_reconstruct_seconds",
        "Time spent in reconstruct_value",
        &["tenant_id", "timeline_id"],
        get_buckets_for_critical_operations(),
    )
    .expect("failed to define a metric")
});

static MATERIALIZED_PAGE_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_materialized_cache_hits_total",
        "Number of cache hits from materialized page cache",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static WAIT_LSN_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_wait_lsn_seconds",
        "Time spent waiting for WAL to arrive",
        &["tenant_id", "timeline_id"],
        get_buckets_for_critical_operations(),
    )
    .expect("failed to define a metric")
});

static LAST_RECORD_LSN: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_last_record_lsn",
        "Last record LSN grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

// Metrics for determining timeline's physical size.
// A layered timeline's physical is defined as the total size of
// (delta/image) layer files on disk.
static CURRENT_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_current_physical_size",
        "Current physical size grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static CURRENT_LOGICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_current_logical_size",
        "Current logical size grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define current logical size metric")
});

// Metrics for cloud upload. These metrics reflect data uploaded to cloud storage,
// or in testing they estimate how much we would upload if we did.
static NUM_PERSISTENT_FILES_CREATED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_created_persistent_files_total",
        "Number of files created that are meant to be uploaded to cloud storage",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static PERSISTENT_BYTES_WRITTEN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_written_persistent_bytes_total",
        "Total bytes written that are meant to be uploaded to cloud storage",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

// Metrics collected on disk IO operations
const STORAGE_IO_TIME_BUCKETS: &[f64] = &[
    0.000001, // 1 usec
    0.00001,  // 10 usec
    0.0001,   // 100 usec
    0.001,    // 1 msec
    0.01,     // 10 msec
    0.1,      // 100 msec
    1.0,      // 1 sec
];

const STORAGE_IO_TIME_OPERATIONS: &[&str] =
    &["open", "close", "read", "write", "seek", "fsync", "gc"];

const STORAGE_IO_SIZE_OPERATIONS: &[&str] = &["read", "write"];

pub static STORAGE_IO_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_io_operations_seconds",
        "Time spent in IO operations",
        &["operation", "tenant_id", "timeline_id"],
        STORAGE_IO_TIME_BUCKETS.into()
    )
    .expect("failed to define a metric")
});

pub static STORAGE_IO_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_io_operations_bytes_total",
        "Total amount of bytes read/written in IO operations",
        &["operation", "tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

const SMGR_QUERY_TIME_OPERATIONS: &[&str] = &[
    "get_rel_exists",
    "get_rel_size",
    "get_page_at_lsn",
    "get_db_size",
];

const SMGR_QUERY_TIME_BUCKETS: &[f64] = &[
    0.00001, // 1/100000 s
    0.0001, 0.00015, 0.0002, 0.00025, 0.0003, 0.00035, 0.0005, 0.00075, // 1/10000 s
    0.001, 0.0025, 0.005, 0.0075, // 1/1000 s
    0.01, 0.0125, 0.015, 0.025, 0.05, // 1/100 s
    0.1,  // 1/10 s
];

pub static SMGR_QUERY_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_smgr_query_seconds",
        "Time spent on smgr query handling",
        &["smgr_query_type", "tenant_id", "timeline_id"],
        SMGR_QUERY_TIME_BUCKETS.into()
    )
    .expect("failed to define a metric")
});

pub static LIVE_CONNECTIONS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_live_connections",
        "Number of live network connections",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric")
});

pub static NUM_ONDISK_LAYERS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
        .expect("failed to define a metric")
});

pub static REMAINING_SYNC_ITEMS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "pageserver_remote_storage_remaining_sync_items",
        "Number of storage sync items left in the queue"
    )
    .expect("failed to register pageserver remote storage remaining sync items int gauge")
});

pub static IMAGE_SYNC_TIME: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "pageserver_remote_storage_image_sync_duration",
        "Time spent to synchronize (up/download) a whole pageserver image",
        &["tenant_id", "timeline_id"],
    )
    .expect("failed to register per-timeline pageserver image sync time vec")
});

pub static IMAGE_SYNC_OPERATION_KINDS: &[&str] = &["upload", "download", "delete"];
pub static IMAGE_SYNC_STATUS: &[&str] = &["success", "failure", "abort"];

pub static IMAGE_SYNC_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_remote_storage_image_sync_count",
        "Number of synchronization operations executed for pageserver images. \
        Grouped by tenant, timeline, operation_kind and status",
        &["tenant_id", "timeline_id", "operation_kind", "status"]
    )
    .expect("failed to register pageserver image sync count vec")
});

pub static IMAGE_SYNC_TIME_HISTOGRAM: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_remote_storage_image_sync_seconds",
        "Time took to synchronize (download or upload) a whole pageserver image. \
        Grouped by operation_kind and status",
        &["operation_kind", "status"],
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 3.0, 10.0, 20.0]
    )
    .expect("failed to register pageserver image sync time histogram vec")
});

pub static REMOTE_INDEX_UPLOAD: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_remote_storage_remote_index_uploads_total",
        "Number of remote index uploads",
        &["tenant_id", "timeline_id"],
    )
    .expect("failed to register pageserver remote index upload vec")
});

pub static NO_LAYERS_UPLOAD: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_remote_storage_no_layers_uploads_total",
        "Number of skipped uploads due to no layers",
        &["tenant_id", "timeline_id"],
    )
    .expect("failed to register pageserver no layers upload vec")
});

pub static TENANT_TASK_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_tenant_task_events",
        "Number of task start/stop/fail events.",
        &["event"],
    )
    .expect("Failed to register tenant_task_events metric")
});

// Metrics collected on WAL redo operations
//
// We collect the time spent in actual WAL redo ('redo'), and time waiting
// for access to the postgres process ('wait') since there is only one for
// each tenant.

/// Time buckets are small because we want to be able to measure the
/// smallest redo processing times. These buckets allow us to measure down
/// to 5us, which equates to 200'000 pages/sec, which equates to 1.6GB/sec.
/// This is much better than the previous 5ms aka 200 pages/sec aka 1.6MB/sec.
macro_rules! redo_histogram_time_buckets {
    () => {
        vec![
            0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001_000,
            0.002_500, 0.005_000, 0.010_000, 0.025_000, 0.050_000, 0.100_000, 0.250_000,
        ]
    };
}

/// While we're at it, also measure the amount of records replayed in each
/// operation. We have a global 'total replayed' counter, but that's not
/// as useful as 'what is the skew for how many records we replay in one
/// operation'.
macro_rules! redo_histogram_count_buckets {
    () => {
        vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]
    };
}

pub static WAL_REDO_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_seconds",
        "Time spent on WAL redo",
        redo_histogram_time_buckets!()
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_WAIT_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_wait_seconds",
        "Time spent waiting for access to the WAL redo process",
        redo_histogram_time_buckets!(),
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_RECORDS_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_records_histogram",
        "Histogram of number of records replayed per redo",
        redo_histogram_count_buckets!(),
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_RECORD_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_replayed_wal_records_total",
        "Number of WAL records replayed in WAL redo process"
    )
    .unwrap()
});

#[derive(Debug)]
pub struct TimelineMetrics {
    tenant_id: String,
    timeline_id: String,
    pub reconstruct_time_histo: Histogram,
    pub materialized_page_cache_hit_counter: GenericCounter<AtomicU64>,
    pub flush_time_histo: Histogram,
    pub compact_time_histo: Histogram,
    pub create_images_time_histo: Histogram,
    pub init_logical_size_histo: Histogram,
    pub load_layer_map_histo: Histogram,
    pub last_record_gauge: IntGauge,
    pub wait_lsn_time_histo: Histogram,
    pub current_physical_size_gauge: UIntGauge,
    /// copy of LayeredTimeline.current_logical_size
    pub current_logical_size_gauge: UIntGauge,
    pub num_persistent_files_created: IntCounter,
    pub persistent_bytes_written: IntCounter,
}

impl TimelineMetrics {
    pub fn new(tenant_id: &TenantId, timeline_id: &TimelineId) -> Self {
        let tenant_id = tenant_id.to_string();
        let timeline_id = timeline_id.to_string();
        let reconstruct_time_histo = RECONSTRUCT_TIME
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let materialized_page_cache_hit_counter = MATERIALIZED_PAGE_CACHE_HIT
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let flush_time_histo = STORAGE_TIME
            .get_metric_with_label_values(&["layer flush", &tenant_id, &timeline_id])
            .unwrap();
        let compact_time_histo = STORAGE_TIME
            .get_metric_with_label_values(&["compact", &tenant_id, &timeline_id])
            .unwrap();
        let create_images_time_histo = STORAGE_TIME
            .get_metric_with_label_values(&["create images", &tenant_id, &timeline_id])
            .unwrap();
        let init_logical_size_histo = STORAGE_TIME
            .get_metric_with_label_values(&["init logical size", &tenant_id, &timeline_id])
            .unwrap();
        let load_layer_map_histo = STORAGE_TIME
            .get_metric_with_label_values(&["load layer map", &tenant_id, &timeline_id])
            .unwrap();
        let last_record_gauge = LAST_RECORD_LSN
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let wait_lsn_time_histo = WAIT_LSN_TIME
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let current_physical_size_gauge = CURRENT_PHYSICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let current_logical_size_gauge = CURRENT_LOGICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let num_persistent_files_created = NUM_PERSISTENT_FILES_CREATED
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let persistent_bytes_written = PERSISTENT_BYTES_WRITTEN
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();

        TimelineMetrics {
            tenant_id,
            timeline_id,
            reconstruct_time_histo,
            materialized_page_cache_hit_counter,
            flush_time_histo,
            compact_time_histo,
            create_images_time_histo,
            init_logical_size_histo,
            load_layer_map_histo,
            last_record_gauge,
            wait_lsn_time_histo,
            current_physical_size_gauge,
            current_logical_size_gauge,
            num_persistent_files_created,
            persistent_bytes_written,
        }
    }
}

impl Drop for TimelineMetrics {
    fn drop(&mut self) {
        let tenant_id = &self.tenant_id;
        let timeline_id = &self.timeline_id;
        let _ = RECONSTRUCT_TIME.remove_label_values(&[tenant_id, timeline_id]);
        let _ = MATERIALIZED_PAGE_CACHE_HIT.remove_label_values(&[tenant_id, timeline_id]);
        let _ = LAST_RECORD_LSN.remove_label_values(&[tenant_id, timeline_id]);
        let _ = WAIT_LSN_TIME.remove_label_values(&[tenant_id, timeline_id]);
        let _ = CURRENT_PHYSICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        let _ = CURRENT_LOGICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        let _ = NUM_PERSISTENT_FILES_CREATED.remove_label_values(&[tenant_id, timeline_id]);
        let _ = PERSISTENT_BYTES_WRITTEN.remove_label_values(&[tenant_id, timeline_id]);

        for op in STORAGE_TIME_OPERATIONS {
            let _ = STORAGE_TIME.remove_label_values(&[op, tenant_id, timeline_id]);
        }
        for op in STORAGE_IO_TIME_OPERATIONS {
            let _ = STORAGE_IO_TIME.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in STORAGE_IO_SIZE_OPERATIONS {
            let _ = STORAGE_IO_SIZE.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in SMGR_QUERY_TIME_OPERATIONS {
            let _ = SMGR_QUERY_TIME.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in IMAGE_SYNC_OPERATION_KINDS {
            for status in IMAGE_SYNC_STATUS {
                let _ = IMAGE_SYNC_COUNT.remove_label_values(&[tenant_id, timeline_id, op, status]);
            }
        }

        let _ = IMAGE_SYNC_TIME.remove_label_values(&[tenant_id, timeline_id]);
    }
}

pub fn remove_tenant_metrics(tenant_id: &TenantId) {
    let _ = STORAGE_TIME.remove_label_values(&["gc", &tenant_id.to_string(), "-"]);
}
