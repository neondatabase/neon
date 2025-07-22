use alloc_metrics::TrackedAllocator;
use proxy::binary::proxy::MemoryContext;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
// Safety: `MemoryContext` upholds the safety requirements.
static GLOBAL: TrackedAllocator<Jemalloc, MemoryContext> =
    unsafe { TrackedAllocator::new(Jemalloc, MemoryContext::Root) };

#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:21\0";

fn main() -> anyhow::Result<()> {
    GLOBAL.register_thread();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| GLOBAL.register_thread())
        .build()
        .expect("Failed building the Runtime")
        .block_on(proxy::binary::proxy::run(&GLOBAL))
}
