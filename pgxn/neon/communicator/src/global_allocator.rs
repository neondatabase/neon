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

use metrics::IntGauge;

struct MyAllocator {
    allocations: AtomicU64,
    deallocations: AtomicU64,

    allocated: AtomicUsize,
    high: AtomicUsize,
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

pub struct MyAllocatorCollector {
    allocations: IntGauge,
    deallocations: IntGauge,
    allocated: IntGauge,
    high: IntGauge,
}

impl MyAllocatorCollector {
    pub fn new() -> MyAllocatorCollector {
        MyAllocatorCollector {
            allocations: IntGauge::new(
                "allocations_total",
                "Number of allocations in Rust code",
            ).unwrap(),
            deallocations: IntGauge::new(
                "deallocations_total",
                "Number of deallocations in Rust code",
            ).unwrap(),
            allocated: IntGauge::new(
                "allocated_total",
                "Bytes currently allocated",
            ).unwrap(),
            high: IntGauge::new(
                "allocated_high",
                "High watermark of allocated bytes",
            ).unwrap(),
        }
    }
}

impl metrics::core::Collector for MyAllocatorCollector {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();

        descs.append(&mut self.allocations.desc());
        descs.append(&mut self.deallocations.desc());
        descs.append(&mut self.allocated.desc());
        descs.append(&mut self.high.desc());

        descs
    }

    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        let mut values = Vec::new();

        // update the gauges
        self.allocations.set(GLOBAL.allocations.load(Ordering::Relaxed) as i64);
        self.deallocations.set(GLOBAL.allocations.load(Ordering::Relaxed) as i64);
        self.allocated.set(GLOBAL.allocated.load(Ordering::Relaxed) as i64);
        self.high.set(GLOBAL.high.load(Ordering::Relaxed) as i64);

        values.append(&mut self.allocations.collect());
        values.append(&mut self.deallocations.collect());
        values.append(&mut self.allocated.collect());
        values.append(&mut self.high.collect());

        values
    }
}
