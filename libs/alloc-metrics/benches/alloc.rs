use std::alloc::{GlobalAlloc, Layout, System, handle_alloc_error};

use alloc_metrics::TrackedAllocator;
use criterion::{
    AxisScale, BatchSize, BenchmarkId as Id, Criterion, PlotConfiguration, Throughput,
    criterion_group, criterion_main,
};
use measured::FixedCardinalityLabel;
use tikv_jemallocator::Jemalloc;

criterion_group!(benches, bench_alloc);
criterion_main!(benches);

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

fn bench_alloc(c: &mut Criterion) {
    const KB: u64 = 1024;
    let sizes = [64, 256, KB, 4 * KB, 16 * KB, KB * KB];

    let mut g = c.benchmark_group("alloc");
    g.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in sizes {
        g.throughput(Throughput::Bytes(size));

        let layout = Layout::from_size_align(size as usize, 8).unwrap();

        let bs = BatchSize::NumBatches(10 + size.ilog2() as u64);

        g.bench_with_input(Id::new("system", size), &layout, |b, layout| {
            b.iter_batched(|| {}, |()| Alloc::new(&System, *layout), bs);
        });
        g.bench_with_input(Id::new("tracked[system]", size), &layout, |b, layout| {
            let _scope = ALLOC_SYSTEM.scope(MemoryContext::Test);
            b.iter_batched(|| {}, |()| Alloc::new(&ALLOC_SYSTEM, *layout), bs);
        });
        g.bench_with_input(Id::new("jemalloc", size), &layout, |b, layout| {
            b.iter_batched(|| {}, |()| Alloc::new(&Jemalloc, *layout), bs);
        });
        g.bench_with_input(Id::new("tracked[jemalloc]", size), &layout, |b, layout| {
            let _scope = ALLOC_JEMALLOC.scope(MemoryContext::Test);
            b.iter_batched(|| {}, |()| Alloc::new(&ALLOC_JEMALLOC, *layout), bs);
        });
    }
    g.finish();

    let mut g = c.benchmark_group("dealloc");
    g.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    for size in sizes {
        g.throughput(Throughput::Bytes(size));

        let layout = Layout::from_size_align(size as usize, 8).unwrap();

        let bs = BatchSize::NumBatches(10 + size.ilog2() as u64);

        g.bench_with_input(Id::new("system", size), &layout, |b, layout| {
            b.iter_batched(|| Alloc::new(&System, *layout), drop, bs)
        });
        g.bench_with_input(Id::new("tracked[system]", size), &layout, |b, layout| {
            let _scope = ALLOC_SYSTEM.scope(MemoryContext::Test);
            b.iter_batched(|| Alloc::new(&ALLOC_SYSTEM, *layout), drop, bs)
        });
        g.bench_with_input(Id::new("jemalloc", size), &layout, |b, layout| {
            b.iter_batched(|| Alloc::new(&Jemalloc, *layout), drop, bs)
        });
        g.bench_with_input(Id::new("tracked[jemalloc]", size), &layout, |b, layout| {
            let _scope = ALLOC_JEMALLOC.scope(MemoryContext::Test);
            b.iter_batched(|| Alloc::new(&ALLOC_JEMALLOC, *layout), drop, bs)
        });
    }
    g.finish();
}
