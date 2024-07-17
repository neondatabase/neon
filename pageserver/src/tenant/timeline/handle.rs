//! # Memory Management
//!
//! Before Timeline shutdown we have a reference cycle
//! [`HandleInner::timeline`] => [`Timeline::handlers`] => [`HandleInner`]..
//!
//! We do this so the hot path need only do one, uncontended, atomic increment,
//! the [`Cache::get`]'s [`Weak<HandleInner>::upgrade`]` call.
//!
//! We must break the reference cyle when either the [`Cache`] or the [`Timeline`]
//! is supposed to shut down:
//!
//! - [`Cache`]: [`Cache::drop`] removes the long-lived strong ref [`Timeline::handlers`].
//! - [`Timeline::handlers`]: [`PerTimelineState::drop`] breaks the reference cycle.
//!
//! Conn shutdown removes the Arc<HandlerTimeline> from Timeline::handlers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::Weak;

use either::Either;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use utils::id::TenantId;
use utils::id::TenantShardTimelineId;
use utils::sync::gate::GateGuard;

use super::Timeline;

/// [`crate::page_service`] uses this to repeatedly, cheaply, get [`Handle`]s to
/// the right [`Timeline`] for a given [`TenantShardTimelineId`].
///
/// Without this, we'd have to go through the [`crate::tenant::mgr`] for each
/// getpage request.
#[derive(Default)]
pub(crate) struct Cache {
    cache: HashMap<TenantShardTimelineId, Weak<HandleInner>>,
}

/// This struct is a reference to [`Timeline`]
/// that keeps the [`Timeline::gate`] open while it exists.
///
/// The idea is that for every getpage request, [`crate::page_service`] acquires
/// one of these through [`Cache::get`], uses it to serve that request, then drops
/// the handle.
pub(crate) struct Handle(Arc<HandleInner>);
struct HandleInner {
    timeline: Arc<Timeline>,
    // The timeline's gate held open.
    _gate_guard: utils::sync::gate::GateGuard,
}

/// This struct embedded into each [`Timeline`] object to keep the [`Cache`]'s
/// [`Weak<HandleInner>`] alive while the [`Timeline`] is alive.
#[derive(Default)]
pub(super) struct PerTimelineState {
    // None = shutting down
    handlers: Mutex<Option<Vec<Arc<HandleInner>>>>,
}

/// We're abstract over the [`crate::tenant::mgr`] so we can test this module.
pub(crate) trait TenantManager {
    type Error;
    /// Invoked by [`Cache::gt`] to resolve a [`TenantShardTimelineId`] to a [`Timeline`].
    /// Errors are returned as [`GetError::TenantManager`].
    fn resolve(&self, key: &TenantShardTimelineId) -> Result<Arc<Timeline>, Self::Error>;
}

/// Errors returned by [`Cache::get`].
pub(crate) enum GetError<M> {
    TenantManager(M),
    TimelineGateClosed,
}

impl Cache {
    /// Get a [`Handle`] for the Timeline identified by `key`.
    /// See [`Cache`] and other structs for more context.
    ///
    /// Note: this method will not fail if the timeline is
    /// [`Timeline::is_stopping`] or [`Timeline::cancel`]led,
    /// but the gate is still open.
    ///
    /// The `Timeline`'s methods, which are invoked through the
    /// returned [`Handle`], are responsible for checking these conditions
    /// and if so, return an error that causes the page service to
    /// close the connection.
    #[instrument(level = "trace", skip_all, fields(%key))]
    pub(crate) fn get<M, E>(
        &mut self,
        key: &TenantShardTimelineId,
        tenant_manager: M,
    ) -> Result<Handle, GetError<E>>
    where
        M: TenantManager<Error = E>,
    {
        let cached: Option<_> = match self.cache.get(&key) {
            None => {
                trace!("cache miss, never seen");
                None
            }
            Some(weak) => match weak.upgrade() {
                None => {
                    trace!("handle cache stale");
                    // clean up cache
                    self.cache.remove(&key).unwrap();
                    None
                }
                Some(timeline) => {
                    trace!("handle cache hit");
                    Some(timeline)
                }
            },
        };
        match cached {
            Some(handle) => Ok(Handle(handle)),
            None => match tenant_manager.resolve(key) {
                Ok(timeline) => {
                    let gate_guard = match timeline.gate.enter() {
                        Ok(guard) => guard,
                        Err(_) => {
                            return Err(GetError::TimelineGateClosed);
                        }
                    };
                    let handle = Arc::new(
                        // TODO: global metric that keeps track of the number of live HandlerTimeline instances
                        // so we can identify reference cycle bugs.
                        HandleInner {
                            _gate_guard: gate_guard,
                            timeline: timeline.clone(),
                        },
                    );
                    {
                        let mut lock_guard =
                            timeline.handlers.handlers.lock().expect("mutex poisoned");
                        match &mut *lock_guard {
                            Some(timeline_handlers_list) => {
                                for handler in timeline_handlers_list.iter() {
                                    assert!(!Arc::ptr_eq(handler, &handle));
                                }
                                timeline_handlers_list.push(Arc::clone(&handle));
                                self.cache.insert(*key, Arc::downgrade(&handle));
                            }
                            None => {
                                return Err(GetError::TimelineGateClosed);
                            }
                        }
                    }
                    Ok(Handle(handle))
                }
                Err(e) => Err(GetError::TenantManager(e)),
            },
        }
    }
}

impl PerTimelineState {
    /// After this method returns, [`Cache::get`] misses will fail.
    /// Already-alive [`Handle`]s keep a cache entry alive.
    /// The expectation is that users of these [`Handle`]s will
    /// observe timeline cancellation when using the [`Timeline`]
    /// for the next time.
    ///
    /// TOOD: we can keep a back-pointer to the [`Cache`] in each
    /// [`HandleInner`] and take it out, Much simpler to do this
    /// using intrusive linked lists :(.
    pub(super) fn shutdown(&self) {
        let mut handlers = self.handlers.lock().expect("mutex poisoned");
        Self::shutdown_impl(&mut handlers);
    }
    fn shutdown_impl(locked: &mut MutexGuard<Option<Vec<Arc<HandleInner>>>>) {
        let Some(handlers) = locked.take() else {
            trace!("PerTimelineState already shut down");
            return;
        };
        let nhandlers = handlers.len();
        trace!(nhandlers, "dropping PerTimelineState");
    }
}

impl Drop for PerTimelineState {
    fn drop(&mut self) {
        let mut handlers = self
            .handlers
            .try_lock()
            .expect("cannot be locked when drop handler runs");
        Self::shutdown_impl(&mut handlers);
    }
}

/// When the page service connection closes and drops its instance of [`Cache`],
/// we have to clean up the [`PerTimelineState`] to break the strong-reference
/// cycle between the [`HandleInner::timeline`] and the [`Timeline::handlers`].
impl Drop for Cache {
    fn drop(&mut self) {
        for (_, cached) in self.cache.drain() {
            if let Some(cached) = cached.upgrade() {
                let per_timeline_state: &PerTimelineState = &cached.timeline.handlers;
                let mut lock_guard = per_timeline_state.handlers.lock().expect("mutex poisoned");
                if let Some(handlers) = &mut *lock_guard {
                    handlers.retain(|handler| !Arc::ptr_eq(handler, &cached));
                }
            }
        }
    }
}

impl std::ops::Deref for Handle {
    type Target = Timeline;
    fn deref(&self) -> &Self::Target {
        &self.0.timeline
    }
}
