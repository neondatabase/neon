//! Defines [`RequestContext`].
//!
//! It is a structure that we use throughout the pageserver to propagate
//! high-level context from places that _originate_ activity down to the
//! shared code paths at the heart of the pageserver. It's inspired by
//! Golang's `context.Context`.
//!
//! For example, in `Timeline::get(page_nr, lsn)` we need to answer the following questions:
//! 1. What high-level activity ([`TaskKind`]) needs this page?
//!    We need that information as a categorical dimension for page access
//!    statistics, which we, in turn, need to guide layer eviction policy design.
//! 2. How should we behave if, to produce the page image, we need to
//!    on-demand download a layer file ([`DownloadBehavior`]).
//!
//! [`RequestContext`] satisfies those needs.
//! The current implementation is a small `struct` that is passed through
//! the call chain by reference.
//!
//! ### Future Work
//!
//! However, we do not intend to stop here, since there are other needs that
//! require carrying information from high to low levels of the app.
//!
//! Most importantly, **cancellation signaling** in response to
//! 1. timeouts (page_service max response time) and
//! 2. lifecycle requests (detach tenant, delete timeline).
//!
//! Related to that, there is sometimes a need to ensure that all tokio tasks spawned
//! by the transitive callees of a request have finished. The keyword here
//! is **Structured Concurrency**, and right now, we use `task_mgr` in most places,
//! `TaskHandle` in some places, and careful code review around `FuturesUnordered`
//! or `JoinSet` in other places.
//!
//! We do not yet have a systematic cancellation story in pageserver, and it is
//! pretty clear that [`RequestContext`] will be responsible for that.
//! So, the API already prepares for this role through the
//! [`RequestContext::detached_child`] and [`RequestContext::attached_child`]  methods.
//! See their doc comments for details on how we will use them in the future.
//!
//! It is not clear whether or how we will enforce Structured Concurrency, and
//! what role [`RequestContext`] will play there.
//! So, the API doesn't prepare us for this topic.
//!
//! Other future uses of `RequestContext`:
//! - Communicate compute & IO priorities (user-initiated request vs. background-loop)
//! - Request IDs for distributed tracing
//! - Request/Timeline/Tenant-scoped log levels
//!
//! RequestContext might look quite different once it supports those features.
//! Likely, it will have a shape similar to Golang's `context.Context`.
//!
//! ### Why A Struct Instead Of Method Parameters
//!
//! What's typical about such information is that it needs to be passed down
//! along the call chain from high level to low level, but few of the functions
//! in the middle need to understand it.
//! Further, it is to be expected that we will need to propagate more data
//! in the future (see the earlier section on future work).
//! Hence, for functions in the middle of the call chain, we have the following
//! requirements:
//! 1. It should be easy to forward the context to callees.
//! 2. To propagate more data from high-level to low-level code, the functions in
//!    the middle should not need to be modified.
//!
//! The solution is to have a container structure ([`RequestContext`]) that
//! carries the information. Functions that don't care about what's in it
//! pass it along to callees.
//!
//! ### Why Not Task-Local Variables
//!
//! One could use task-local variables (the equivalent of thread-local variables)
//! to address the immediate needs outlined above.
//! However, we reject task-local variables because:
//! 1. they are implicit, thereby making it harder to trace the data flow in code
//!    reviews and during debugging,
//! 2. they can be mutable, which enables implicit return data flow,
//! 3. they are restrictive in that code which fans out into multiple tasks,
//!    or even threads, needs to carefully propagate the state.
//!
//! In contrast, information flow with [`RequestContext`] is
//! 1. always explicit,
//! 2. strictly uni-directional because RequestContext is immutable,
//! 3. tangible because a [`RequestContext`] is just a value.
//!    When creating child activities, regardless of whether it's a task,
//!    thread, or even an RPC to another service, the value can
//!    be used like any other argument.
//!
//! The solution is that all code paths are infected with precisely one
//! [`RequestContext`] argument. Functions in the middle of the call chain
//! only need to pass it on.

use std::{sync::Arc, time::Duration};

use once_cell::sync::Lazy;
use tracing::warn;
use utils::{id::TimelineId, shard::TenantShardId};

use crate::{
    metrics::{StorageIoSizeMetrics, TimelineMetrics},
    task_mgr::TaskKind,
    tenant::Timeline,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use std::future::Future;
use tracing_utils::perf_span::{PerfInstrument, PerfSpan};

use tracing::{Dispatch, Span};

// The main structure of this module, see module-level comment.
pub struct RequestContext {
    task_kind: TaskKind,
    download_behavior: DownloadBehavior,
    access_stats_behavior: AccessStatsBehavior,
    page_content_kind: PageContentKind,
    read_path_debug: bool,
    scope: Scope,
    perf_span: Option<PerfSpan>,
    perf_span_dispatch: Option<Dispatch>,
}

#[derive(Clone)]
pub(crate) enum Scope {
    Global {
        io_size_metrics: &'static crate::metrics::StorageIoSizeMetrics,
    },
    SecondaryTenant {
        io_size_metrics: &'static crate::metrics::StorageIoSizeMetrics,
    },
    SecondaryTimeline {
        io_size_metrics: crate::metrics::StorageIoSizeMetrics,
    },
    Timeline {
        // We wrap the `Arc<TimelineMetrics>`s inside another Arc to avoid child
        // context creation contending for the ref counters of the Arc<TimelineMetrics>,
        // which are shared among all tasks that operate on the timeline, especially
        // concurrent page_service connections.
        #[allow(clippy::redundant_allocation)]
        arc_arc: Arc<Arc<TimelineMetrics>>,
    },
    #[cfg(test)]
    UnitTest {
        io_size_metrics: &'static crate::metrics::StorageIoSizeMetrics,
    },
    DebugTools {
        io_size_metrics: &'static crate::metrics::StorageIoSizeMetrics,
    },
}

static GLOBAL_IO_SIZE_METRICS: Lazy<crate::metrics::StorageIoSizeMetrics> =
    Lazy::new(|| crate::metrics::StorageIoSizeMetrics::new("*", "*", "*"));

impl Scope {
    pub(crate) fn new_global() -> Self {
        Scope::Global {
            io_size_metrics: &GLOBAL_IO_SIZE_METRICS,
        }
    }
    /// NB: this allocates, so, use only at relatively long-lived roots, e.g., at start
    /// of a compaction iteration.
    pub(crate) fn new_timeline(timeline: &Timeline) -> Self {
        Scope::Timeline {
            arc_arc: Arc::new(Arc::clone(&timeline.metrics)),
        }
    }
    pub(crate) fn new_page_service_pagestream(
        timeline_handle: &crate::tenant::timeline::handle::Handle<
            crate::page_service::TenantManagerTypes,
        >,
    ) -> Self {
        Scope::Timeline {
            arc_arc: Arc::clone(&timeline_handle.metrics),
        }
    }
    pub(crate) fn new_secondary_timeline(
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
    ) -> Self {
        // TODO(https://github.com/neondatabase/neon/issues/11156): secondary timelines have no infrastructure for metrics lifecycle.

        let tenant_id = tenant_shard_id.tenant_id.to_string();
        let shard_id = tenant_shard_id.shard_slug().to_string();
        let timeline_id = timeline_id.to_string();

        let io_size_metrics =
            crate::metrics::StorageIoSizeMetrics::new(&tenant_id, &shard_id, &timeline_id);
        Scope::SecondaryTimeline { io_size_metrics }
    }
    pub(crate) fn new_secondary_tenant(_tenant_shard_id: &TenantShardId) -> Self {
        // Before propagating metrics via RequestContext, the labels were inferred from file path.
        // The only user of VirtualFile at tenant scope is the heatmap download & read.
        // The inferred labels for the path of the heatmap file on local disk were that of the global metric (*,*,*).
        // Thus, we do the same here, and extend that for anything secondary-tenant scoped.
        //
        // If we want to have (tenant_id, shard_id, '*') labels for secondary tenants in the future,
        // we will need to think about the metric lifecycle, i.e., remove them during secondary tenant shutdown,
        // like we do for attached timelines. (We don't have attached-tenant-scoped usage of VirtualFile
        // at this point, so, we were able to completely side-step tenant-scoped stuff there).
        Scope::SecondaryTenant {
            io_size_metrics: &GLOBAL_IO_SIZE_METRICS,
        }
    }
    #[cfg(test)]
    pub(crate) fn new_unit_test() -> Self {
        Scope::UnitTest {
            io_size_metrics: &GLOBAL_IO_SIZE_METRICS,
        }
    }

    pub(crate) fn new_debug_tools() -> Self {
        Scope::DebugTools {
            io_size_metrics: &GLOBAL_IO_SIZE_METRICS,
        }
    }
}

/// The kind of access to the page cache.
#[derive(Clone, Copy, PartialEq, Eq, Debug, enum_map::Enum, strum_macros::IntoStaticStr)]
pub enum PageContentKind {
    Unknown,
    DeltaLayerSummary,
    DeltaLayerBtreeNode,
    DeltaLayerValue,
    ImageLayerSummary,
    ImageLayerBtreeNode,
    ImageLayerValue,
    InMemoryLayer,
}

/// Desired behavior if the operation requires an on-demand download
/// to proceed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DownloadBehavior {
    /// Download the layer file. It can take a while.
    Download,

    /// Download the layer file, but print a warning to the log. This should be used
    /// in code where the layer file is expected to already exist locally.
    Warn,

    /// Return a PageReconstructError::NeedsDownload error
    Error,
}

/// Whether this request should update access times used in LRU eviction
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AccessStatsBehavior {
    /// Update access times: this request's access to data should be taken
    /// as a hint that the accessed layer is likely to be accessed again
    Update,

    /// Do not update access times: this request is accessing the layer
    /// but does not want to indicate that the layer should be retained in cache,
    /// perhaps because the requestor is a compaction routine that will soon cover
    /// this layer with another.
    Skip,
}

pub struct RequestContextBuilder {
    inner: RequestContext,
}

impl RequestContextBuilder {
    /// A new builder with default settings
    pub fn new(task_kind: TaskKind) -> Self {
        Self {
            inner: RequestContext {
                task_kind,
                download_behavior: DownloadBehavior::Download,
                access_stats_behavior: AccessStatsBehavior::Update,
                page_content_kind: PageContentKind::Unknown,
                read_path_debug: false,
                scope: Scope::new_global(),
                perf_span: None,
                perf_span_dispatch: None,
            },
        }
    }

    pub fn from(original: &RequestContext) -> Self {
        Self {
            inner: original.clone(),
        }
    }

    pub fn task_kind(mut self, k: TaskKind) -> Self {
        self.inner.task_kind = k;
        self
    }

    /// Configure the DownloadBehavior of the context: whether to
    /// download missing layers, and/or warn on the download.
    pub fn download_behavior(mut self, b: DownloadBehavior) -> Self {
        self.inner.download_behavior = b;
        self
    }

    /// Configure the AccessStatsBehavior of the context: whether layer
    /// accesses should update the access time of the layer.
    pub(crate) fn access_stats_behavior(mut self, b: AccessStatsBehavior) -> Self {
        self.inner.access_stats_behavior = b;
        self
    }

    pub(crate) fn page_content_kind(mut self, k: PageContentKind) -> Self {
        self.inner.page_content_kind = k;
        self
    }

    pub(crate) fn read_path_debug(mut self, b: bool) -> Self {
        self.inner.read_path_debug = b;
        self
    }

    pub(crate) fn scope(mut self, s: Scope) -> Self {
        self.inner.scope = s;
        self
    }

    pub(crate) fn perf_span_dispatch(mut self, dispatch: Option<Dispatch>) -> Self {
        self.inner.perf_span_dispatch = dispatch;
        self
    }

    pub fn root_perf_span<Fn>(mut self, make_span: Fn) -> Self
    where
        Fn: FnOnce() -> Span,
    {
        assert!(self.inner.perf_span.is_none());
        assert!(self.inner.perf_span_dispatch.is_some());

        let dispatcher = self.inner.perf_span_dispatch.as_ref().unwrap();
        let new_span = tracing::dispatcher::with_default(dispatcher, make_span);

        self.inner.perf_span = Some(PerfSpan::new(new_span, dispatcher.clone()));

        self
    }

    pub fn perf_span<Fn>(mut self, make_span: Fn) -> Self
    where
        Fn: FnOnce(&Span) -> Span,
    {
        if let Some(ref perf_span) = self.inner.perf_span {
            assert!(self.inner.perf_span_dispatch.is_some());
            let dispatcher = self.inner.perf_span_dispatch.as_ref().unwrap();

            let new_span =
                tracing::dispatcher::with_default(dispatcher, || make_span(perf_span.inner()));

            self.inner.perf_span = Some(PerfSpan::new(new_span, dispatcher.clone()));
        }

        self
    }

    pub fn root(self) -> RequestContext {
        self.inner
    }

    pub fn attached_child(self) -> RequestContext {
        self.inner
    }

    pub fn detached_child(self) -> RequestContext {
        self.inner
    }
}

impl RequestContext {
    /// Private clone implementation
    ///
    /// Callers should use the [`RequestContextBuilder`] or child spaning APIs of
    /// [`RequestContext`].
    fn clone(&self) -> Self {
        Self {
            task_kind: self.task_kind,
            download_behavior: self.download_behavior,
            access_stats_behavior: self.access_stats_behavior,
            page_content_kind: self.page_content_kind,
            read_path_debug: self.read_path_debug,
            scope: self.scope.clone(),
            perf_span: self.perf_span.clone(),
            perf_span_dispatch: self.perf_span_dispatch.clone(),
        }
    }

    /// Create a new RequestContext that has no parent.
    ///
    /// The function is called `new` because, once we add children
    /// to it using `detached_child` or `attached_child`, the context
    /// form a tree (not implemented yet since cancellation will be
    /// the first feature that requires a tree).
    ///
    /// # Future: Cancellation
    ///
    /// The only reason why a context like this one can be canceled is
    /// because someone explicitly canceled it.
    /// It has no parent, so it cannot inherit cancellation from there.
    pub fn new(task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        RequestContextBuilder::new(task_kind)
            .download_behavior(download_behavior)
            .root()
    }

    /// Create a detached child context for a task that may outlive `self`.
    ///
    /// Use this when spawning new background activity that should complete
    /// even if the current request is canceled.
    ///
    /// # Future: Cancellation
    ///
    /// Cancellation of `self` will not propagate to the child context returned
    /// by this method.
    ///
    /// # Future: Structured Concurrency
    ///
    /// We could add the Future as a parameter to this function, spawn it as a task,
    /// and pass to the new task the child context as an argument.
    /// That would be an ergonomic improvement.
    ///
    /// We could make new calls to this function fail if `self` is already canceled.
    pub fn detached_child(&self, task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        RequestContextBuilder::from(self)
            .task_kind(task_kind)
            .download_behavior(download_behavior)
            .detached_child()
    }

    /// Create a child of context `self` for a task that shall not outlive `self`.
    ///
    /// Use this when fanning-out work to other async tasks.
    ///
    /// # Future: Cancellation
    ///
    /// Cancelling a context will propagate to its attached children.
    ///
    /// # Future: Structured Concurrency
    ///
    /// We could add the Future as a parameter to this function, spawn it as a task,
    /// and track its `JoinHandle` inside the `RequestContext`.
    ///
    /// We could then provide another method to allow waiting for all child tasks
    /// to finish.
    ///
    /// We could make new calls to this function fail if `self` is already canceled.
    /// Alternatively, we could allow the creation but not spawn the task.
    /// The method to wait for child tasks would return an error, indicating
    /// that the child task was not started because the context was canceled.
    pub fn attached_child(&self) -> Self {
        RequestContextBuilder::from(self).attached_child()
    }

    /// Use this function when you should be creating a child context using
    /// [`attached_child`] or [`detached_child`], but your caller doesn't provide
    /// a context and you are unwilling to change all callers to provide one.
    ///
    /// Before we add cancellation, we should get rid of this method.
    ///
    /// [`attached_child`]: Self::attached_child
    /// [`detached_child`]: Self::detached_child
    pub fn todo_child(task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        Self::new(task_kind, download_behavior)
    }

    pub fn with_scope_timeline(&self, timeline: &Arc<Timeline>) -> Self {
        RequestContextBuilder::from(self)
            .scope(Scope::new_timeline(timeline))
            .attached_child()
    }

    pub(crate) fn with_scope_page_service_pagestream(
        &self,
        timeline_handle: &crate::tenant::timeline::handle::Handle<
            crate::page_service::TenantManagerTypes,
        >,
    ) -> Self {
        RequestContextBuilder::from(self)
            .scope(Scope::new_page_service_pagestream(timeline_handle))
            .attached_child()
    }

    pub fn with_scope_secondary_timeline(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
    ) -> Self {
        RequestContextBuilder::from(self)
            .scope(Scope::new_secondary_timeline(tenant_shard_id, timeline_id))
            .attached_child()
    }

    pub fn with_scope_secondary_tenant(&self, tenant_shard_id: &TenantShardId) -> Self {
        RequestContextBuilder::from(self)
            .scope(Scope::new_secondary_tenant(tenant_shard_id))
            .attached_child()
    }

    #[cfg(test)]
    pub fn with_scope_unit_test(&self) -> Self {
        RequestContextBuilder::from(self)
            .task_kind(TaskKind::UnitTest)
            .scope(Scope::new_unit_test())
            .attached_child()
    }

    pub fn with_scope_debug_tools(&self) -> Self {
        RequestContextBuilder::from(self)
            .task_kind(TaskKind::DebugTool)
            .scope(Scope::new_debug_tools())
            .attached_child()
    }

    pub fn task_kind(&self) -> TaskKind {
        self.task_kind
    }

    pub fn download_behavior(&self) -> DownloadBehavior {
        self.download_behavior
    }

    pub(crate) fn access_stats_behavior(&self) -> AccessStatsBehavior {
        self.access_stats_behavior
    }

    pub(crate) fn page_content_kind(&self) -> PageContentKind {
        self.page_content_kind
    }

    pub(crate) fn read_path_debug(&self) -> bool {
        self.read_path_debug
    }

    pub(crate) fn io_size_metrics(&self) -> &StorageIoSizeMetrics {
        match &self.scope {
            Scope::Global { io_size_metrics } => {
                let is_unit_test = cfg!(test);
                let is_regress_test_build = cfg!(feature = "testing");
                if is_unit_test || is_regress_test_build {
                    panic!("all VirtualFile instances are timeline-scoped");
                } else {
                    use once_cell::sync::Lazy;
                    use std::sync::Mutex;
                    use std::time::Duration;
                    use utils::rate_limit::RateLimit;
                    static LIMIT: Lazy<Mutex<RateLimit>> =
                        Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(1))));
                    let mut guard = LIMIT.lock().unwrap();
                    guard.call2(|rate_limit_stats| {
                        warn!(
                            %rate_limit_stats,
                            backtrace=%std::backtrace::Backtrace::force_capture(),
                            "all VirtualFile instances are timeline-scoped",
                        );
                    });

                    io_size_metrics
                }
            }
            Scope::Timeline { arc_arc } => &arc_arc.storage_io_size,
            Scope::SecondaryTimeline { io_size_metrics } => io_size_metrics,
            Scope::SecondaryTenant { io_size_metrics } => io_size_metrics,
            #[cfg(test)]
            Scope::UnitTest { io_size_metrics } => io_size_metrics,
            Scope::DebugTools { io_size_metrics } => io_size_metrics,
        }
    }

    pub(crate) fn ondemand_download_wait_observe(&self, duration: Duration) {
        if duration == Duration::ZERO {
            return;
        }

        match &self.scope {
            Scope::Timeline { arc_arc } => arc_arc
                .wait_ondemand_download_time
                .observe(self.task_kind, duration),
            _ => {
                use once_cell::sync::Lazy;
                use std::sync::Mutex;
                use std::time::Duration;
                use utils::rate_limit::RateLimit;
                static LIMIT: Lazy<Mutex<RateLimit>> =
                    Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(1))));
                let mut guard = LIMIT.lock().unwrap();
                guard.call2(|rate_limit_stats| {
                    warn!(
                        %rate_limit_stats,
                        backtrace=%std::backtrace::Backtrace::force_capture(),
                        "ondemand downloads should always happen within timeline scope",
                    );
                });
            }
        }
    }

    pub(crate) fn perf_follows_from(&self, from: &RequestContext) {
        if let (Some(span), Some(from_span)) = (&self.perf_span, &from.perf_span) {
            span.inner().follows_from(from_span.inner());
        }
    }

    pub(crate) fn has_perf_span(&self) -> bool {
        self.perf_span.is_some()
    }
}

/// [`Future`] extension trait that allow for creating performance
/// spans on sampled requests
pub(crate) trait PerfInstrumentFutureExt<'a>: Future + Send {
    /// Instrument this future with a new performance span when the
    /// provided request context indicates the originator request
    /// was sampled. Otherwise, just box the future and return it as is.
    fn maybe_perf_instrument<Fn>(
        self,
        ctx: &RequestContext,
        make_span: Fn,
    ) -> BoxFuture<'a, Self::Output>
    where
        Self: Sized + 'a,
        Fn: FnOnce(&Span) -> Span,
    {
        match &ctx.perf_span {
            Some(perf_span) => {
                assert!(ctx.perf_span_dispatch.is_some());
                let dispatcher = ctx.perf_span_dispatch.as_ref().unwrap();

                let new_span =
                    tracing::dispatcher::with_default(dispatcher, || make_span(perf_span.inner()));

                let new_perf_span = PerfSpan::new(new_span, dispatcher.clone());
                self.instrument(new_perf_span).boxed()
            }
            None => self.boxed(),
        }
    }
}

// Implement the trait for all types that satisfy the trait bounds
impl<'a, T: Future + Send + 'a> PerfInstrumentFutureExt<'a> for T {}
