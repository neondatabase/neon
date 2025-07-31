//! Global allocator, for tracking memory usage of the Rust parts
//!
//! Postgres is designed to handle allocation failure (ie. malloc() returning NULL) gracefully.  It
//! rolls backs the transaction and gives the user an "ERROR: out of memory" error. Rust code
//! however panics if an allocation fails. We don't want that to ever happen, because an unhandled
//! panic leads to Postgres crash and restart. Our strategy is to pre-allocate a large enough chunk
//! of memory for use by the Rust code, so that the allocations never fail.
//!
//! To pick the size for the pre-allocated chunk, we have a metric to track the high watermark
//! memory usage of all the Rust allocations in total.
//!
//! TODO:
//!
//! - Currently we just export the metrics. Actual allocations are still just passed through to
//!   the system allocator.
//! - Take padding etc. overhead into account

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use measured::{Gauge, MetricGroup};
use measured::metric::gauge::GaugeState;
use measured::metric::MetricEncoding;
use measured::metric;

pub(crate) struct MyAllocator {
    allocations: AtomicU64,
    deallocations: AtomicU64,

    allocated: AtomicUsize,
    high: AtomicUsize,
}

#[derive(MetricGroup)]
#[metric(new())]
struct MyAllocatorMetricGroup {
    /// Number of allocations in Rust code
    communicator_mem_allocations: Gauge,

    /// Number of deallocations in Rust code
    communicator_mem_deallocations: Gauge,

    /// Bytes currently allocated
    communicator_mem_allocated: Gauge,

    /// High watermark of allocated bytes
    communicator_mem_high: Gauge,
}

unsafe impl GlobalAlloc for MyAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        let mut allocated = self.allocated.fetch_add(layout.size(), Ordering::Relaxed);
        allocated += layout.size();
        self.high.fetch_max(allocated, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        self.allocated.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: MyAllocator = MyAllocator {
    allocations: AtomicU64::new(0),
    deallocations: AtomicU64::new(0),
    allocated: AtomicUsize::new(0),
    high: AtomicUsize::new(0),
};

pub(crate) struct MyAllocatorCollector {
    metrics: MyAllocatorMetricGroup,
}

impl MyAllocatorCollector {
    pub(crate) fn new() -> Self {
        Self {
            metrics: MyAllocatorMetricGroup::new(),
        }
    }
}

impl <T: metric::group::Encoding> MetricGroup<T> for MyAllocatorCollector
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), <T as metric::group::Encoding>::Err> {
        // Update the gauges with fresh values first
        self.metrics.communicator_mem_allocations
            .set(GLOBAL.allocations.load(Ordering::Relaxed) as i64);
        self.metrics.communicator_mem_deallocations
            .set(GLOBAL.allocations.load(Ordering::Relaxed) as i64);
        self.metrics.communicator_mem_allocated
            .set(GLOBAL.allocated.load(Ordering::Relaxed) as i64);
        self.metrics.communicator_mem_high.set(GLOBAL.high.load(Ordering::Relaxed) as i64);

        self.metrics.collect_group_into(enc)
    }
}
