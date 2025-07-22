use std::alloc::{GlobalAlloc, Layout, System, handle_alloc_error};

use alloc_metrics::TrackedAllocator;
use criterion::{
    AxisScale, BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, measurement::Measurement,
};
use measured::FixedCardinalityLabel;
use tikv_jemallocator::Jemalloc;

fn main() {
    let mut c = Criterion::default().configure_from_args();
    bench(&mut c);
    c.final_summary();
}

#[rustfmt::skip]
fn bench(c: &mut Criterion) {
    bench_alloc(c.benchmark_group("alloc/system"),  &System, &ALLOC_SYSTEM);
    bench_alloc(c.benchmark_group("alloc/jemalloc"), &Jemalloc, &ALLOC_JEMALLOC);

    bench_dealloc(c.benchmark_group("dealloc/system"), &System, &ALLOC_SYSTEM);
    bench_dealloc(c.benchmark_group("dealloc/jemalloc"), &Jemalloc, &ALLOC_JEMALLOC);
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
#[label(singleton = "memory_context")]
pub enum MemoryContext {
    Root,
    Test,
}

static ALLOC_SYSTEM: TrackedAllocator<System, MemoryContext> =
    unsafe { TrackedAllocator::new(System, MemoryContext::Root) };
static ALLOC_JEMALLOC: TrackedAllocator<Jemalloc, MemoryContext> =
    unsafe { TrackedAllocator::new(Jemalloc, MemoryContext::Root) };

const KB: u64 = 1024;
const SIZES: [u64; 6] = [64, 256, KB, 4 * KB, 16 * KB, KB * KB];

fn bench_alloc<A: GlobalAlloc>(
    mut g: BenchmarkGroup<'_, impl Measurement>,
    alloc1: &'static A,
    alloc2: &'static TrackedAllocator<A, MemoryContext>,
) {
    g.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in SIZES {
        let layout = Layout::from_size_align(size as usize, 8).unwrap();

        g.throughput(criterion::Throughput::Bytes(size));
        g.bench_with_input(BenchmarkId::new("default", size), &layout, |b, &layout| {
            let bs = criterion::BatchSize::NumBatches(10 + size.ilog2() as u64);
            b.iter_batched(|| {}, |()| Alloc::new(alloc1, layout), bs);
        });
        g.bench_with_input(BenchmarkId::new("tracked", size), &layout, |b, &layout| {
            let _scope = alloc2.scope(MemoryContext::Test);

            let bs = criterion::BatchSize::NumBatches(10 + size.ilog2() as u64);
            b.iter_batched(|| {}, |()| Alloc::new(alloc2, layout), bs);
        });
    }
}

fn bench_dealloc<A: GlobalAlloc>(
    mut g: BenchmarkGroup<'_, impl Measurement>,
    alloc1: &'static A,
    alloc2: &'static TrackedAllocator<A, MemoryContext>,
) {
    g.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in SIZES {
        let layout = Layout::from_size_align(size as usize, 8).unwrap();

        g.throughput(criterion::Throughput::Bytes(size));
        g.bench_with_input(BenchmarkId::new("default", size), &layout, |b, &layout| {
            let bs = criterion::BatchSize::NumBatches(10 + size.ilog2() as u64);
            b.iter_batched(|| Alloc::new(alloc1, layout), drop, bs);
        });
        g.bench_with_input(BenchmarkId::new("tracked", size), &layout, |b, &layout| {
            let _scope = alloc2.scope(MemoryContext::Test);

            let bs = criterion::BatchSize::NumBatches(10 + size.ilog2() as u64);
            b.iter_batched(|| Alloc::new(alloc2, layout), drop, bs);
        });
    }
}

struct Alloc<'a, A: GlobalAlloc> {
    alloc: &'a A,
    ptr: *mut u8,
    layout: Layout,
}

impl<'a, A: GlobalAlloc> Alloc<'a, A> {
    fn new(alloc: &'a A, layout: Layout) -> Self {
        let ptr = unsafe { alloc.alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }

        // actually make the page resident.
        unsafe { ptr.cast::<u8>().write(1) };

        Self { alloc, ptr, layout }
    }
}

impl<'a, A: GlobalAlloc> Drop for Alloc<'a, A> {
    fn drop(&mut self) {
        unsafe { self.alloc.dealloc(self.ptr, self.layout) };
    }
}
