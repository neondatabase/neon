//!
//! Support for profiling
//!
//! This relies on a modified version of the 'pprof-rs' crate. That's not very
//! nice, so to avoid a hard dependency on that, this is an optional feature.
//!
use crate::config::{PageServerConf, ProfilingConfig};

/// The actual implementation is in the `profiling_impl` submodule. If the profiling
/// feature is not enabled, it's just a dummy implementation that panics if you
/// try to enabled profiling in the configuration.
pub use profiling_impl::*;

#[cfg(feature = "profiling")]
mod profiling_impl {
    use super::*;
    use pprof;
    use std::marker::PhantomData;

    /// Start profiling the current thread. Returns a guard object;
    /// the profiling continues until the guard is dropped.
    ///
    /// Note: profiling is not re-entrant. If you call 'profpoint_start' while
    /// profiling is already started, nothing happens, and the profiling will be
    /// stopped when either guard object is dropped.
    #[inline]
    pub fn profpoint_start(
        conf: &crate::config::PageServerConf,
        point: ProfilingConfig,
    ) -> Option<ProfilingGuard> {
        if conf.profiling == point {
            pprof::start_profiling();
            Some(ProfilingGuard(PhantomData))
        } else {
            None
        }
    }

    /// A hack to remove Send and Sync from the ProfilingGuard. Because the
    /// profiling is attached to current thread.
    ////
    /// See comments in https://github.com/rust-lang/rust/issues/68318
    type PhantomUnsend = std::marker::PhantomData<*mut u8>;

    pub struct ProfilingGuard(PhantomUnsend);

    impl Drop for ProfilingGuard {
        fn drop(&mut self) {
            pprof::stop_profiling();
        }
    }

    /// Initialize the profiler. This must be called before any 'profpoint_start' calls.
    pub fn init_profiler(conf: &PageServerConf) -> Option<pprof::ProfilerGuard> {
        if conf.profiling != ProfilingConfig::Disabled {
            Some(pprof::ProfilerGuardBuilder::default().build().unwrap())
        } else {
            None
        }
    }

    /// Exit the profiler. Writes the flamegraph to current workdir.
    pub fn exit_profiler(_conf: &PageServerConf, profiler_guard: &Option<pprof::ProfilerGuard>) {
        // Write out the flamegraph
        if let Some(profiler_guard) = profiler_guard {
            if let Ok(report) = profiler_guard.report().build() {
                // this gets written under the workdir
                let file = std::fs::File::create("flamegraph.svg").unwrap();
                let mut options = pprof::flamegraph::Options::default();
                options.image_width = Some(2500);
                report.flamegraph_with_options(file, &mut options).unwrap();
            }
        }
    }
}

/// Dummy implementation when compiling without profiling feature or for non-linux OSes.
#[cfg(not(feature = "profiling"))]
mod profiling_impl {
    use super::*;

    pub struct DummyProfilerGuard;

    pub fn profpoint_start(
        _conf: &PageServerConf,
        _point: ProfilingConfig,
    ) -> Option<DummyProfilerGuard> {
        None
    }

    pub fn init_profiler(conf: &PageServerConf) -> Option<DummyProfilerGuard> {
        if conf.profiling != ProfilingConfig::Disabled {
            // shouldn't happen, we don't allow profiling in the config if the support
            // for it is disabled.
            panic!("profiling enabled but the binary was compiled without profiling support");
        }
        None
    }

    pub fn exit_profiler(_conf: &PageServerConf, _guard: &Option<DummyProfilerGuard>) {}
}
