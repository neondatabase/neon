
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;


use futures::Future;

use once_cell::sync::Lazy;
use pageserver_api::config::{
    PageServicePipeliningConfig, PageServicePipeliningConfigPipelined,
    PageServiceProtocolPipelinedBatchingStrategy, PageServiceProtocolPipelinedExecutionStrategy,
};
use pageserver_api::shard::TenantShardId;

use strum_macros::{IntoStaticStr, VariantNames};
use utils::id::TimelineId;

use crate::config;
use crate::config::PageServerConf;

use crate::task_mgr::TaskKind;

use crate::tenant::mgr::TenantSlot;
use crate::tenant::storage_layer::PersistentLayerDesc;
use crate::tenant::throttle::ThrottleResult;



// Metrics collected on operations on the storage repository.
#[derive(Debug, VariantNames, IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum StorageTimeOperation {

    
   
}








#[derive(IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum PageCacheErrorKind {
    AcquirePinnedSlotTimeout,
}

pub(crate) fn page_cache_errors_inc(_error_kind: PageCacheErrorKind) {
}





pub(crate) mod wait_ondemand_download_time {

    pub(crate) fn shutdown_timeline(_tenant_id: &str, _shard_id: &str, _timeline_id: &str) {
    }

    pub(crate) fn preinitialize_global_metrics() {
    }
}



#[derive(
    strum_macros::EnumIter,
    strum_macros::EnumString,
    strum_macros::Display,
    strum_macros::IntoStaticStr,
)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum LayerKind {
    Delta,
    Image,
}

#[derive(
    strum_macros::EnumIter,
    strum_macros::EnumString,
    strum_macros::Display,
    strum_macros::IntoStaticStr,
)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum LayerLevel {
    // We don't track the currently open ephemeral layer, since there's always exactly 1 and its
    // size changes. See `TIMELINE_EPHEMERAL_BYTES`.
    Frozen,
    L0,
    L1,
}














pub(crate) mod initial_logical_size {
    use metrics::{IntCounter, register_int_counter};
    use once_cell::sync::Lazy;

    #[derive(strum_macros::IntoStaticStr)]
    pub(crate) enum StartCircumstances {
        SkippedConcurrencyLimiter,
        AfterBackgroundTasksRateLimit,
    }


    // context: https://github.com/neondatabase/neon/issues/5963
    pub(crate) static TIMELINES_WHERE_WALRECEIVER_GOT_APPROXIMATE_SIZE: Lazy<IntCounter> =
        Lazy::new(|| {
            register_int_counter!(
                "pageserver_initial_logical_size_timelines_where_walreceiver_got_approximate_size",
                "Counter for the following event: walreceiver calls\
                 Timeline::get_current_logical_size() and it returns `Approximate` for the first time."
            )
            .unwrap()
        });
}




/// Metrics related to the lifecycle of a [`crate::tenant::Tenant`] object: things
/// like how long it took to load.
///
/// Note that these are process-global metrics, _not_ per-tenant metrics.  Per-tenant
/// metrics are rather expensive, and usually fine grained stuff makes more sense
/// at a timeline level than tenant level.
pub(crate) struct TenantMetrics {
   
   
   
   
}

pub(crate) static TENANT: Lazy<TenantMetrics> = Lazy::new(|| {
    TenantMetrics {
   
    
    
   
}
});


/// VirtualFile fs operation variants.
///
/// Operations:
/// - open ([`std::fs::OpenOptions::open`])
/// - close (dropping [`crate::virtual_file::VirtualFile`])
/// - close-by-replace (close by replacement algorithm)
/// - read (`read_at`)
/// - write (`write_at`)
/// - seek (modify internal position or file length query)
/// - fsync ([`std::fs::File::sync_all`])
/// - metadata ([`std::fs::File::metadata`])
#[derive(
    Debug, Clone, Copy, strum_macros::EnumCount, strum_macros::EnumIter, strum_macros::FromRepr,
)]
pub(crate) enum StorageIoOperation {
    Open,
    OpenAfterReplace,
    Close,
    CloseByReplace,
    Read,
    Write,
    Seek,
    Fsync,
    Metadata,
}


#[cfg(not(test))]
pub(crate) mod virtual_file_descriptor_cache {
  


    // SIZE_CURRENT: derive it like so:
    // ```
    // sum (pageserver_io_operations_seconds_count{operation=~"^(open|open-after-replace)$")
    // -ignoring(operation)
    // sum(pageserver_io_operations_seconds_count{operation=~"^(close|close-by-replace)$"}
    // ```
}

#[cfg(not(test))]
pub(crate) mod virtual_file_io_engine {
}

pub(crate) struct SmgrOpTimer(Option<SmgrOpTimerInner>);
pub(crate) struct SmgrOpTimerInner {
    timings: SmgrOpTimerState,
}

/// The stages of request processing are represented by the enum variants.
/// Used as part of [`SmgrOpTimerInner::timings`].
///
/// Request processing calls into the `SmgrOpTimer::observe_*` methods at the
/// transition points.
/// These methods bump relevant counters and then update [`SmgrOpTimerInner::timings`]
/// to the next state.
///
/// Each request goes through every stage, in all configurations.
///
#[derive(Debug)]
enum SmgrOpTimerState {
    Received {
        // In the future, we may want to track the full time the request spent
        // inside pageserver process (time spent in kernel buffers can't be tracked).
        // `received_at` would be used for that.
        #[allow(dead_code)]
        received_at: Instant,
    },
    Throttling {
    },
    // NB: when adding observation points, remember to update the Drop impl.
}

// NB: when adding observation points, remember to update the Drop impl.
impl SmgrOpTimer {
    /// See [`SmgrOpTimerState`] for more context.
    pub(crate) fn observe_throttle_start(&mut self, _at: Instant) {
        let Some(inner) = self.0.as_mut() else {
            return;
        };
        let SmgrOpTimerState::Received { received_at: _ } = &mut inner.timings else {
            return;
        };
        inner.timings = SmgrOpTimerState::Throttling {
        };
    }

    /// See [`SmgrOpTimerState`] for more context.
    pub(crate) fn observe_throttle_done(&mut self, _throttle: ThrottleResult) {
    }


}



#[derive(
    Debug,
    Clone,
    Copy,
    IntoStaticStr,
    strum_macros::EnumCount,
    strum_macros::EnumIter,
    strum_macros::FromRepr,
    enum_map::Enum,
)]
#[strum(serialize_all = "snake_case")]
pub enum SmgrQueryType {
    GetRelExists,
    GetRelSize,
    GetPageAtLsn,
    GetDbSize,
    GetSlruSegment,
    #[cfg(feature = "testing")]
    Test,
}

#[derive(
    Debug,
    Clone,
    Copy,
    IntoStaticStr,
    strum_macros::EnumCount,
    strum_macros::EnumIter,
    strum_macros::FromRepr,
    enum_map::Enum,
)]
#[strum(serialize_all = "snake_case")]
pub enum GetPageBatchBreakReason {
    BatchFull,
    NonBatchableRequest,
    NonUniformLsn,
    SamePageAtDifferentLsn,
    NonUniformTimeline,
    ExecutorSteal,
    #[cfg(feature = "testing")]
    NonUniformKey,
}

pub(crate) struct SmgrQueryTimePerTimeline {
}


















fn set_page_service_config_max_batch_size(conf: &PageServicePipeliningConfig) {
    let (_label_values, _value) = match conf {
        PageServicePipeliningConfig::Serial => (["serial", "-", "-"], 1),
        PageServicePipeliningConfig::Pipelined(PageServicePipeliningConfigPipelined {
            max_batch_size,
            execution,
            batching,
        }) => {
            let mode = "pipelined";
            let execution = match execution {
                PageServiceProtocolPipelinedExecutionStrategy::ConcurrentFutures => {
                    "concurrent-futures"
                }
                PageServiceProtocolPipelinedExecutionStrategy::Tasks => "tasks",
            };
            let batching = match batching {
                PageServiceProtocolPipelinedBatchingStrategy::UniformLsn => "uniform-lsn",
                PageServiceProtocolPipelinedBatchingStrategy::ScatteredLsn => "scattered-lsn",
            };

            ([mode, execution, batching], max_batch_size.get())
        }
    };
}








impl SmgrQueryTimePerTimeline {
    pub(crate) fn new(
        _tenant_shard_id: &TenantShardId,
        _timeline_id: &TimelineId,
        _pagestream_throttle_metrics: Arc<tenant_throttling::Pagestream>,
    ) -> Self {
        
        Self {
        }
    }
    pub(crate) fn start_smgr_op(&self, _op: SmgrQueryType, received_at: Instant) -> SmgrOpTimer {
        
        SmgrOpTimer(Some(SmgrOpTimerInner {
            timings: SmgrOpTimerState::Received { received_at },
        }))
    }

}



#[derive(Clone, Copy, enum_map::Enum, IntoStaticStr)]
pub(crate) enum ComputeCommandKind {
    PageStreamV3,
    PageStreamV2,
    Basebackup,
    Fullbackup,
    LeaseLsn,
}





pub(crate) struct TenantManagerMetrics {
   
}

impl TenantManagerMetrics {
    /// Helpers for tracking slots.  Note that these do not track the lifetime of TenantSlot objects
    /// exactly: they track the lifetime of the slots _in the tenant map_.
    pub(crate) fn slot_inserted(&self, _slot: &TenantSlot) {
    }

    pub(crate) fn slot_removed(&self, _slot: &TenantSlot) {
    }

    #[cfg(all(debug_assertions, not(test)))]
    pub(crate) fn slots_total(&self) -> u64 {
        // self.tenant_slots_attached.get()
            // + self.tenant_slots_secondary.get()
            // + self.tenant_slots_inprogress.get()
        0
    }
}

pub(crate) static TENANT_MANAGER: Lazy<TenantManagerMetrics> = Lazy::new(|| {
    TenantManagerMetrics {
        
    }
});



#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteOpKind {
    Upload,
    Download,
    Delete,
}
impl RemoteOpKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum RemoteOpFileKind {
    Layer,
    Index,
}
impl RemoteOpFileKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Layer => "layer",
            Self::Index => "index",
        }
    }
}



// walreceiver metrics





















#[derive(Debug, enum_map::Enum, strum_macros::IntoStaticStr)]
pub(crate) enum WalRedoKillCause {
    WalRedoProcessDrop,
    NoLeakChildDrop,
    Startup,
}






pub(crate) struct TimelineMetrics {
    tenant_id: String,
    shard_id: String,
    timeline_id: String,
    
   
    /// copy of LayeredTimeline.current_logical_size
    shutdown: std::sync::atomic::AtomicBool,
}

impl TimelineMetrics {
    pub fn new(
        tenant_shard_id: &TenantShardId,
        timeline_id_raw: &TimelineId,
    ) -> Self {
        let tenant_id = tenant_shard_id.tenant_id.to_string();
        let shard_id = format!("{}", tenant_shard_id.shard_slug());
        let timeline_id = timeline_id_raw.to_string();
    
         

        TimelineMetrics {
            tenant_id,
            shard_id,
            timeline_id,
            
           
          
            shutdown: std::sync::atomic::AtomicBool::default(),
        }
    }

    /// Removes a persistent layer from TIMELINE_LAYER metrics.
    pub fn dec_layer(&self, _layer_desc: &PersistentLayerDesc) {
    }

    /// Adds a persistent layer to TIMELINE_LAYER metrics.
    pub fn inc_layer(&self, _layer_desc: &PersistentLayerDesc) {
    }

    pub(crate) fn shutdown(&self) {
        let was_shutdown = self
            .shutdown
            .swap(true, std::sync::atomic::Ordering::Relaxed);

        if was_shutdown {
            // this happens on tenant deletion because tenant first shuts down timelines, then
            // invokes timeline deletion which first shuts down the timeline again.
            // TODO: this can be removed once https://github.com/neondatabase/neon/issues/5080
            return;
        }

        let tenant_id = &self.tenant_id;
        let timeline_id = &self.timeline_id;
        let shard_id = &self.shard_id;
        


        // The following metrics are born outside of the TimelineMetrics lifecycle but still
        // removed at the end of it. The idea is to have the metrics outlive the
        // entity during which they're observed, e.g., the smgr metrics shall
        // outlive an individual smgr connection, but not the timeline.

        

        

        wait_ondemand_download_time::shutdown_timeline(tenant_id, shard_id, timeline_id);

        
        
    }
}



/// Wrapper future that measures the time spent by a remote storage operation,
/// and records the time and success/failure as a prometheus metric.
pub(crate) trait MeasureRemoteOp<O, E>: Sized + Future<Output = Result<O, E>> {
    async fn measure_remote_op(
        self,
        _task_kind: Option<TaskKind>, // not all caller contexts have a RequestContext / TaskKind handy
        _file_kind: RemoteOpFileKind,
        _op: RemoteOpKind,
    ) -> Result<O, E> {
        
        self.await
        
    }
}

impl<Fut, O, E> MeasureRemoteOp<O, E> for Fut where Fut: Sized + Future<Output = Result<O, E>> {}

pub mod tokio_epoll_uring {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use metrics::{Histogram, LocalHistogram, UIntGauge, register_histogram};
    use once_cell::sync::Lazy;

    /// Shared storage for tokio-epoll-uring thread local metrics.
    pub(crate) static THREAD_LOCAL_METRICS_STORAGE: Lazy<ThreadLocalMetricsStorage> =
        Lazy::new(|| {
            let slots_submission_queue_depth = register_histogram!(
                "pageserver_tokio_epoll_uring_slots_submission_queue_depth",
                "The slots waiters queue depth of each tokio_epoll_uring system",
                vec![
                    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0
                ],
            )
            .expect("failed to define a metric");
            ThreadLocalMetricsStorage {
                observers: Mutex::new(HashMap::new()),
                slots_submission_queue_depth,
            }
        });

    pub struct ThreadLocalMetricsStorage {
        /// List of thread local metrics observers.
        observers: Mutex<HashMap<u64, Arc<ThreadLocalMetrics>>>,
        /// A histogram shared between all thread local systems
        /// for collecting slots submission queue depth.
        slots_submission_queue_depth: Histogram,
    }

    /// Each thread-local [`tokio_epoll_uring::System`] gets one of these as its
    /// [`tokio_epoll_uring::metrics::PerSystemMetrics`] generic.
    ///
    /// The System makes observations into [`Self`] and periodically, the collector
    /// comes along and flushes [`Self`] into the shared storage [`THREAD_LOCAL_METRICS_STORAGE`].
    ///
    /// [`LocalHistogram`] is `!Send`, so, we need to put it behind a [`Mutex`].
    /// But except for the periodic flush, the lock is uncontended so there's no waiting
    /// for cache coherence protocol to get an exclusive cache line.
    pub struct ThreadLocalMetrics {
        /// Local observer of thread local tokio-epoll-uring system's slots waiters queue depth.
        slots_submission_queue_depth: Mutex<LocalHistogram>,
    }

    impl ThreadLocalMetricsStorage {
        /// Registers a new thread local system. Returns a thread local metrics observer.
        pub fn register_system(&self, id: u64) -> Arc<ThreadLocalMetrics> {
            let per_system_metrics = Arc::new(ThreadLocalMetrics::new(
                self.slots_submission_queue_depth.local(),
            ));
            let mut g = self.observers.lock().unwrap();
            g.insert(id, Arc::clone(&per_system_metrics));
            per_system_metrics
        }

        /// Removes metrics observer for a thread local system.
        /// This should be called before dropping a thread local system.
        pub fn remove_system(&self, id: u64) {
            let mut g = self.observers.lock().unwrap();
            g.remove(&id);
        }

        /// Flush all thread local metrics to the shared storage.
        pub fn flush_thread_local_metrics(&self) {
            let g = self.observers.lock().unwrap();
            g.values().for_each(|local| {
                local.flush();
            });
        }
    }

    impl ThreadLocalMetrics {
        pub fn new(slots_submission_queue_depth: LocalHistogram) -> Self {
            ThreadLocalMetrics {
                slots_submission_queue_depth: Mutex::new(slots_submission_queue_depth),
            }
        }

        /// Flushes the thread local metrics to shared aggregator.
        pub fn flush(&self) {
            let Self {
                slots_submission_queue_depth,
            } = self;
            slots_submission_queue_depth.lock().unwrap().flush();
        }
    }

    impl tokio_epoll_uring::metrics::PerSystemMetrics for ThreadLocalMetrics {
        fn observe_slots_submission_queue_depth(&self, _queue_depth: u64) {
        }
    }

    pub struct Collector {
        descs: Vec<metrics::core::Desc>,
    }

    impl metrics::core::Collector for Collector {
        fn desc(&self) -> Vec<&metrics::core::Desc> {
            self.descs.iter().collect()
        }

        fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
            Vec::with_capacity(Self::NMETRICS)
        }
    }

    impl Collector {
        const NMETRICS: usize = 3;

        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            let mut descs = Vec::new();

            let systems_created = UIntGauge::new(
                "pageserver_tokio_epoll_uring_systems_created",
                "counter of tokio-epoll-uring systems that were created",
            )
            .unwrap();
            descs.extend(
                metrics::core::Collector::desc(&systems_created)
                    .into_iter()
                    .cloned(),
            );

            let systems_destroyed = UIntGauge::new(
                "pageserver_tokio_epoll_uring_systems_destroyed",
                "counter of tokio-epoll-uring systems that were destroyed",
            )
            .unwrap();
            descs.extend(
                metrics::core::Collector::desc(&systems_destroyed)
                    .into_iter()
                    .cloned(),
            );

            Self {
                descs,
            }
        }
    }

   
}
pub(crate) mod tenant_throttling {
    use metrics::register_int_counter_vec;
    use once_cell::sync::Lazy;
    use utils::shard::TenantShardId;

    pub(crate) struct Metrics<const KIND: usize> {
    }

    static COUNT_ACCOUNTED_START: Lazy<metrics::IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "pageserver_tenant_throttling_count_accounted_start_global",
            "Count of tenant throttling starts, by kind of throttle.",
            &["kind"]
        )
        .unwrap()
    });
  
    static COUNT_ACCOUNTED_FINISH: Lazy<metrics::IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "pageserver_tenant_throttling_count_accounted_finish_global",
            "Count of tenant throttling finishes, by kind of throttle.",
            &["kind"]
        )
        .unwrap()
    });
  
    static WAIT_USECS: Lazy<metrics::IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "pageserver_tenant_throttling_wait_usecs_sum_global",
            "Sum of microseconds that spent waiting throttle by kind of throttle.",
            &["kind"]
        )
        .unwrap()
    });
   

    static WAIT_COUNT: Lazy<metrics::IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "pageserver_tenant_throttling_count_global",
            "Count of tenant throttlings, by kind of throttle.",
            &["kind"]
        )
        .unwrap()
    });
    

    
    pub type Pagestream = Metrics<0>;

    impl<const KIND: usize> Metrics<KIND> {
        pub(crate) fn new(_tenant_shard_id: &TenantShardId) -> Self {
            Metrics {
            }
        }
    }

    pub(crate) fn preinitialize_global_metrics() {
        Lazy::force(&COUNT_ACCOUNTED_START);
        Lazy::force(&COUNT_ACCOUNTED_FINISH);
        Lazy::force(&WAIT_USECS);
        Lazy::force(&WAIT_COUNT);
    }

}

pub(crate) mod disk_usage_based_eviction {

   
    

}


pub(crate) fn set_tokio_runtime_setup(_setup: &str, _num_threads: NonZeroUsize) {
}

pub fn preinitialize_metrics(
    conf: &'static PageServerConf,
    _ignored: config::ignored_fields::Paths,
) {
    set_page_service_config_max_batch_size(&conf.page_service_pipelining);

    // counters

    // Deletion queue stats

    // Tenant stats
    Lazy::force(&TENANT);

    // Tenant manager stats
    Lazy::force(&TENANT_MANAGER);




    // gauges
    // WALRECEIVER_ACTIVE_MANAGERS.get(); // This seems like a read, not a modification, leaving it for now.

    // histograms
    

    // Custom
  

    tenant_throttling::preinitialize_global_metrics();
    wait_ondemand_download_time::preinitialize_global_metrics();
}
