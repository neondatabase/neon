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

use crate::task_mgr::TaskKind;

// The main structure of this module, see module-level comment.
#[derive(Debug, Default)]
pub struct RequestContext {
    latency_recording: Option<latency_recording::LatencyRecording>,
    io_concurrency: Option<io_concurrency_propagation::IoConcurrencyPropagation>,
    cancel: Option<cancellation::Cancellation>,
}

trait Propagatable: Default {
    fn propagate(&self, child: &mut Self);
}

impl RequestContext {
    fn root() -> Self {
        Self::default()
    }
    fn child(&self) -> RequestContext {
        let mut child = RequestContext::default();
        let Self {
            latency_recording,
        } = self;
        if let Some(latency_recording) = latency_recording {
            child.latency_recording = Some(latency_recording.child());
        }
    }
}

mod latency_recording {
    struct LatencyRecording {
        inner: Arc<Mutex<Inner>>,
    }

    impl LatencyRecording {
        fn new() -> Self {
            Self {
                current: Mutex::new(HashMap::new()),
            }
        }

        fn on_request_recv(&self, now: Instant) -> {
            let mut inner = self.inner.lock().unwrap();
            inner.current.insert(now, now);
        }

    }

    impl Propagatable for LatencyRecording {
        fn propagate(&self, other: &mut Self) {
            let mut inner = self.inner.lock().unwrap();
            let other_inner = other.get_mut();
            for (k, v) in other_inner.current.iter() {
                inner.current.insert(*k, *v);
            }
        }
    }
}

mod io_concurrency_propagation {

    struct IoConcurrencyPropagation {
        inner: IoConcurrency,
    }

    impl Propagatable for IoConcurrencyPropagation {
        fn propagate(&self, other: &mut Self) {
            other.inner = self.inner.clone();
        }
    }

}

mod cancellation {

    struct Cancellation {
        sources: Vec<CancellationToken>,
    }

    impl Propagatable for Cancellation {
        fn propagate(&self, other: &mut Self) {
            other.sources.extend(self.sources.iter().map(|tok| tok.child_toke()));
        }
    }

    impl Cancellation {
        async fn cancelled(&self) {
            // TODO: this one is quite inefficient, it allocates
            // But it's clear something better can be built within this architecture.
            futures::future::select_all(self.sources.iter().map(|tok| tok.cancelled()))
        }
    }

}
