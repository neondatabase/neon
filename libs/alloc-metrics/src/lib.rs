//! Tagged allocator measurements.

mod metric_vec;
mod thread_local;

use std::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
    marker::PhantomData,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use measured::{
    FixedCardinalityLabel, LabelGroup, MetricGroup,
    label::StaticLabelSet,
    metric::{MetricEncoding, counter::CounterState, group::Encoding, name::MetricName},
};
use metrics::{CounterPairAssoc, MeasuredCounterPairState};
use thread_local::ThreadLocal;

use crate::metric_vec::DenseCounterPairVec;

type AllocCounter<T> = DenseCounterPairVec<AllocPair<T>, T>;

pub struct TrackedAllocator<A, T: 'static + Send + Sync + FixedCardinalityLabel + LabelGroup> {
    inner: A,

    /// potentially high-content fallback if the thread was not registered.
    default_counters: MeasuredCounterPairState,
    /// Default tag to use if this thread is not registered.
    default_tag: T,

    /// Current memory context for this thread.
    thread_scope: OnceLock<ThreadLocal<Cell<T>>>,
    /// per thread state containing low contention counters for faster allocations.
    thread_state: OnceLock<ThreadLocal<ThreadState<T>>>,

    /// where thread alloc data is eventually saved to, even if threads are shutdown.
    global: OnceLock<AllocCounter<T>>,
}

impl<A, T> TrackedAllocator<A, T>
where
    T: 'static + Send + Sync + FixedCardinalityLabel + LabelGroup,
{
    /// # Safety
    ///
    /// [`FixedCardinalityLabel`] must be implemented correctly, fully dense, and must not panic.
    pub const unsafe fn new(alloc: A, default: T) -> Self {
        TrackedAllocator {
            inner: alloc,
            default_tag: default,
            default_counters: MeasuredCounterPairState {
                inc: CounterState {
                    count: AtomicU64::new(0),
                },
                dec: CounterState {
                    count: AtomicU64::new(0),
                },
            },
            thread_scope: OnceLock::new(),
            thread_state: OnceLock::new(),
            global: OnceLock::new(),
        }
    }

    /// Allocations
    pub fn register_thread(&'static self) {
        self.register_thread_inner();
    }

    pub fn scope(&'static self, tag: T) -> AllocScope<'static, T> {
        let cell = self.register_thread_inner();
        let last = cell.replace(tag);
        AllocScope { cell, last }
    }

    fn register_thread_inner(&'static self) -> &'static Cell<T> {
        self.thread_state
            .get_or_init(ThreadLocal::new)
            .get_or(|| ThreadState {
                counters: DenseCounterPairVec::default(),
                global: self.global.get_or_init(DenseCounterPairVec::default),
            });

        self.thread_scope
            .get_or_init(ThreadLocal::new)
            .get_or(|| Cell::new(self.default_tag))
    }

    fn current_counters_alloc_safe(&self) -> Option<&AllocCounter<T>> {
        // We are being very careful here to not allocate or panic.
        self.thread_state
            .get()
            .and_then(ThreadLocal::get)
            .map(|s| &s.counters)
            .or_else(|| self.global.get())
    }

    fn current_tag_alloc_safe(&self) -> T {
        // We are being very careful here to not allocate or panic.
        self.thread_scope
            .get()
            .and_then(ThreadLocal::get)
            .map_or(self.default_tag, Cell::get)
    }
}

impl<A, T> TrackedAllocator<A, T>
where
    T: 'static + Send + Sync + FixedCardinalityLabel + LabelGroup,
{
    unsafe fn alloc_inner(&self, layout: Layout, alloc: impl FnOnce(Layout) -> *mut u8) -> *mut u8 {
        let Ok((tagged_layout, tag_offset)) = layout.extend(Layout::new::<T>()) else {
            return std::ptr::null_mut();
        };
        let tagged_layout = tagged_layout.pad_to_align();

        // Safety: The layout is not zero-sized.
        let ptr = alloc(tagged_layout);

        // allocation failed.
        if ptr.is_null() {
            return ptr;
        }

        let tag = self.current_tag_alloc_safe();

        // Allocation successful. Write our tag
        // Safety: tag_offset is inbounds of the ptr
        unsafe { ptr.add(tag_offset).cast::<T>().write(tag) }

        let metric = if let Some(counters) = self.current_counters_alloc_safe() {
            // safety: caller ensured that <T as FixedCardinalitySet> is implemented correctly.
            let id = unsafe { counters.vec.try_with_labels(tag).unwrap_unchecked() };
            counters.vec.get_metric(id)
        } else {
            // if tag is not default, then global would have been registered, therefore tag must be default.
            &self.default_counters
        };

        metric.inc_by(layout.size() as u64);

        ptr
    }
}

// We will tag our allocation by adding `T` to the end of the layout.
// This is ok only as long as it does not overflow. If it does, we will
// just fail the allocation by returning null.
//
// Safety: we will not unwind during alloc, and we will ensure layouts are handled correctly.
unsafe impl<A, T> GlobalAlloc for TrackedAllocator<A, T>
where
    A: GlobalAlloc,
    T: 'static + Send + Sync + FixedCardinalityLabel + LabelGroup,
{
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // safety: same as caller
        unsafe { self.alloc_inner(layout, |tagged_layout| self.inner.alloc(tagged_layout)) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // safety: same as caller
        unsafe {
            self.alloc_inner(layout, |tagged_layout| {
                self.inner.alloc_zeroed(tagged_layout)
            })
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller must ensure that the `new_size` does not overflow.
        // `layout.align()` comes from a `Layout` and is thus guaranteed to be valid.
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };

        let Ok((new_tagged_layout, new_tag_offset)) = new_layout.extend(Layout::new::<T>()) else {
            return std::ptr::null_mut();
        };
        let new_tagged_layout = new_tagged_layout.pad_to_align();

        let Ok((tagged_layout, tag_offset)) = layout.extend(Layout::new::<T>()) else {
            // Safety: This layout clearly did not match what was originally allocated,
            // otherwise alloc() would have caught this error and returned null.
            unsafe { std::hint::unreachable_unchecked() }
        };
        let tagged_layout = tagged_layout.pad_to_align();

        // get the tag set during alloc
        // Safety: tag_offset is inbounds of the ptr
        let tag = unsafe { ptr.add(tag_offset).cast::<T>().read() };

        // Safety: layout sizes are correct
        let new_ptr = unsafe {
            self.inner
                .realloc(ptr, tagged_layout, new_tagged_layout.size())
        };

        // allocation failed.
        if new_ptr.is_null() {
            return new_ptr;
        }

        let new_tag = self.current_tag_alloc_safe();

        // Allocation successful. Write our tag
        // Safety: new_tag_offset is inbounds of the ptr
        unsafe { new_ptr.add(new_tag_offset).cast::<T>().write(new_tag) }

        let (new_metric, old_metric) = if let Some(counters) = self.current_counters_alloc_safe() {
            // safety: caller ensured that <T as FixedCardinalitySet> is implemented correctly.
            let new_id = unsafe { counters.vec.try_with_labels(new_tag).unwrap_unchecked() };
            // safety: caller ensured that <T as FixedCardinalitySet> is implemented correctly.
            let old_id = unsafe { counters.vec.try_with_labels(tag).unwrap_unchecked() };
            let new_metric = counters.vec.get_metric(new_id);
            let old_metric = counters.vec.get_metric(old_id);

            (new_metric, old_metric)
        } else {
            // no tag was registered at all, therefore both tags must be default.
            (&self.default_counters, &self.default_counters)
        };

        let (inc, dec) = if tag.encode() != new_tag.encode() {
            (new_layout.size() as u64, layout.size() as u64)
        } else if new_layout.size() > layout.size() {
            ((new_layout.size() - layout.size()) as u64, 0)
        } else {
            (0, (layout.size() - new_layout.size()) as u64)
        };

        new_metric.inc.inc_by(inc);
        old_metric.dec.inc_by(dec);

        new_ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let Ok((tagged_layout, tag_offset)) = layout.extend(Layout::new::<T>()) else {
            // Safety: This layout clearly did not match what was originally allocated,
            // otherwise alloc() would have caught this error and returned null.
            unsafe { std::hint::unreachable_unchecked() }
        };
        let tagged_layout = tagged_layout.pad_to_align();

        // get the tag set during alloc
        // Safety: tag_offset is inbounds of the ptr
        let tag = unsafe { ptr.add(tag_offset).cast::<T>().read() };

        // Safety: caller upholds contract for us
        unsafe { self.inner.dealloc(ptr, tagged_layout) }

        let metric = if let Some(counters) = self.current_counters_alloc_safe() {
            // safety: caller ensured that <T as FixedCardinalitySet> is implemented correctly.
            let id = unsafe { counters.vec.try_with_labels(tag).unwrap_unchecked() };
            counters.vec.get_metric(id)
        } else {
            // if tag is not default, then global would have been registered, therefore tag must be default.
            &self.default_counters
        };

        metric.dec_by(layout.size() as u64);
    }
}

pub struct AllocScope<'a, T: FixedCardinalityLabel> {
    cell: &'a Cell<T>,
    last: T,
}

impl<'a, T: FixedCardinalityLabel> Drop for AllocScope<'a, T> {
    fn drop(&mut self) {
        self.cell.set(self.last);
    }
}

struct AllocPair<T>(PhantomData<T>);

impl<T: FixedCardinalityLabel + LabelGroup> CounterPairAssoc for AllocPair<T> {
    const INC_NAME: &'static MetricName = MetricName::from_str("allocated_bytes");
    const DEC_NAME: &'static MetricName = MetricName::from_str("deallocated_bytes");

    const INC_HELP: &'static str = "total number of bytes allocated";
    const DEC_HELP: &'static str = "total number of bytes deallocated";

    type LabelGroupSet = StaticLabelSet<T>;
}

struct ThreadState<T: 'static + FixedCardinalityLabel + LabelGroup> {
    counters: AllocCounter<T>,
    global: &'static AllocCounter<T>,
}

// Ensure the counters are measured on thread destruction.
impl<T: 'static + FixedCardinalityLabel + LabelGroup> Drop for ThreadState<T> {
    fn drop(&mut self) {
        // iterate over all labels
        for tag in (0..T::cardinality()).map(T::decode) {
            // load and reset the counts in the thread-local counters.
            let id = self.counters.vec.with_labels(tag);
            let m = self.counters.vec.get_metric_mut(id);
            let inc = *m.inc.count.get_mut();
            let dec = *m.dec.count.get_mut();

            // add the counts into the global counters.
            let id = self.global.vec.with_labels(tag);
            let m = self.global.vec.get_metric(id);
            m.inc.count.fetch_add(inc, Relaxed);
            m.dec.count.fetch_add(dec, Relaxed);
        }
    }
}

impl<A, T, Enc> MetricGroup<Enc> for TrackedAllocator<A, T>
where
    T: 'static + Send + Sync + FixedCardinalityLabel + LabelGroup,
    Enc: Encoding,
    CounterState: MetricEncoding<Enc>,
{
    fn collect_group_into(&self, enc: &mut Enc) -> Result<(), Enc::Err> {
        let global = self.global.get_or_init(DenseCounterPairVec::default);

        // iterate over all counter threads
        for s in self.thread_state.get().into_iter().flat_map(|s| s.iter()) {
            // iterate over all labels
            for tag in (0..T::cardinality()).map(T::decode) {
                let id = s.counters.vec.with_labels(tag);
                sample(global, s.counters.vec.get_metric(id), tag);
            }
        }

        sample(global, &self.default_counters, self.default_tag);

        global.collect_group_into(enc)
    }
}

fn sample<T: FixedCardinalityLabel + LabelGroup>(
    global: &AllocCounter<T>,
    local: &MeasuredCounterPairState,
    tag: T,
) {
    // load and reset the counts in the thread-local counters.
    let inc = local.inc.count.swap(0, Relaxed);
    let dec = local.dec.count.swap(0, Relaxed);

    // add the counts into the global counters.
    let id = global.vec.with_labels(tag);
    let m = global.vec.get_metric(id);
    m.inc.count.fetch_add(inc, Relaxed);
    m.dec.count.fetch_add(dec, Relaxed);
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};

    use measured::{FixedCardinalityLabel, MetricGroup, text::BufferedTextEncoder};

    use crate::TrackedAllocator;

    #[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
    #[label(singleton = "memory_context")]
    pub enum MemoryContext {
        Root,
        Test,
    }

    #[test]
    fn alloc() {
        // Safety: `MemoryContext` upholds the safety requirements.
        static GLOBAL: TrackedAllocator<System, MemoryContext> =
            unsafe { TrackedAllocator::new(System, MemoryContext::Root) };

        GLOBAL.register_thread();

        let _test = GLOBAL.scope(MemoryContext::Test);

        let ptr = unsafe { GLOBAL.alloc(Layout::for_value(&[0_i32])) };
        let ptr = unsafe { GLOBAL.realloc(ptr, Layout::for_value(&[0_i32]), 8) };

        drop(_test);

        let ptr = unsafe { GLOBAL.realloc(ptr, Layout::for_value(&[0_i32, 1_i32]), 4) };
        unsafe { GLOBAL.dealloc(ptr, Layout::for_value(&[0_i32])) };

        let mut text = BufferedTextEncoder::new();
        GLOBAL.collect_group_into(&mut text).unwrap();
        let text = String::from_utf8(text.finish().into()).unwrap();
        assert_eq!(
            text,
            r#"# HELP deallocated_bytes total number of bytes deallocated
# TYPE deallocated_bytes counter
deallocated_bytes{memory_context="root"} 4
deallocated_bytes{memory_context="test"} 8

# HELP allocated_bytes total number of bytes allocated
# TYPE allocated_bytes counter
allocated_bytes{memory_context="root"} 4
allocated_bytes{memory_context="test"} 8
"#
        );
    }
}
