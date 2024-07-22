//! # Memory Management
//!
//! Before Timeline shutdown we have a reference cycle
//! [`HandleInner::timeline`] => [`Timeline::handles`] => [`HandleInner`]..
//!
//! We do this so the hot path need only do one, uncontended, atomic increment,
//! the [`Cache::get`]'s [`Weak<HandleInner>::upgrade`]` call.
//!
//! The reference cycle breaks when the [`HandleInner`] gets dropped.
//! The expectation is that [`HandleInner`]s are short-lived.
//! See [`PerTimelineState::shutdown`] for more details on this expectation.

use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use pageserver_api::shard::ShardIdentity;
use tracing::instrument;
use tracing::trace;
use utils::id::TimelineId;
use utils::shard::ShardIndex;
use utils::shard::ShardNumber;

use crate::tenant::mgr::ShardSelector;

pub(crate) trait Types: Sized {
    type TenantManagerError: Sized;
    type TenantManager: TenantManager<Self> + Sized;
    type Timeline: ArcTimeline<Self> + Sized;
}

/// [`crate::page_service`] uses this to repeatedly, cheaply, get [`Handle`]s to
/// the right [`Timeline`] for a given [`ShardTimelineId`].
///
/// Without this, we'd have to go through the [`crate::tenant::mgr`] for each
/// getpage request.
pub(crate) struct Cache<T: Types> {
    map: Map<T>,
}

type Map<T> = HashMap<ShardTimelineId, Weak<HandleInner<T>>>;

impl<T: Types> Default for Cache<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone, Copy)]
pub(crate) struct ShardTimelineId {
    pub(crate) shard_index: ShardIndex,
    pub(crate) timeline_id: TimelineId,
}

/// This struct is a reference to [`Timeline`]
/// that keeps the [`Timeline::gate`] open while it exists.
///
/// The idea is that for every getpage request, [`crate::page_service`] acquires
/// one of these through [`Cache::get`], uses it to serve that request, then drops
/// the handle.
pub(crate) struct Handle<T: Types>(Arc<HandleInner<T>>);
struct HandleInner<T: Types> {
    shut_down: AtomicBool,
    timeline: T::Timeline,
    // The timeline's gate held open.
    _gate_guard: utils::sync::gate::GateGuard,
}

/// This struct embedded into each [`Timeline`] object to keep the [`Cache`]'s
/// [`Weak<HandleInner>`] alive while the [`Timeline`] is alive.
pub struct PerTimelineState<T: Types> {
    // None = shutting down
    handles: Mutex<Option<Vec<Arc<HandleInner<T>>>>>,
}

impl<T: Types> Default for PerTimelineState<T> {
    fn default() -> Self {
        Self {
            handles: Mutex::new(Some(Vec::default())),
        }
    }
}

/// We're abstract over the [`crate::tenant::mgr`] so we can test this module.
pub(crate) trait TenantManager<T: Types> {
    /// Invoked by [`Cache::get`] to resolve a [`ShardTimelineId`] to a [`Timeline`].
    /// Errors are returned as [`GetError::TenantManager`].
    async fn resolve(
        &self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> Result<T::Timeline, T::TenantManagerError>;
}

pub(crate) trait ArcTimeline<T: Types>: Clone {
    fn gate(&self) -> &utils::sync::gate::Gate;
    fn shard_timeline_id(&self) -> ShardTimelineId;
    fn get_shard_identity(&self) -> &ShardIdentity;
    fn per_timeline_state(&self) -> &PerTimelineState<T>;
}

/// Errors returned by [`Cache::get`].
pub(crate) enum GetError<T: Types> {
    TenantManager(T::TenantManagerError),
    TimelineGateClosed,
    PerTimelineStateShutDown,
}

enum RoutingResult<T: Types> {
    FastPath(Handle<T>),
    Index(ShardIndex),
    NeedConsultTenantManager,
}

impl<T: Types> Cache<T> {
    /// Get a [`Handle`] for the Timeline identified by `key`.
    ///
    /// The returned [`Handle`] must be short-lived so that
    /// the [`Timeline::gate`] is not held open longer than necessary.
    ///
    /// Note: this method will not fail if the timeline is
    /// [`Timeline::is_stopping`] or [`Timeline::cancel`]led,
    /// but the gate is still open.
    ///
    /// The `Timeline`'s methods, which are invoked through the
    /// returned [`Handle`], are responsible for checking these conditions
    /// and if so, return an error that causes the page service to
    /// close the connection.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn get(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        let handle = self
            .get_impl(timeline_id, shard_selector, tenant_manager)
            .await?;
        if handle.0.shut_down.load(Ordering::Relaxed) {
            return Err(GetError::PerTimelineStateShutDown);
        }
        Ok(handle)
    }

    #[instrument(level = "trace", skip_all)]
    async fn get_impl(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        let miss: ShardSelector = {
            let routing_state = self.shard_routing(shard_selector);
            match routing_state {
                RoutingResult::FastPath(handle) => return Ok(handle),
                RoutingResult::Index(shard_index) => {
                    let key = ShardTimelineId {
                        shard_index,
                        timeline_id,
                    };
                    match self.map.get(&key) {
                        Some(cached) => match cached.upgrade() {
                            Some(upgraded) => return Ok(Handle(upgraded)),
                            None => {
                                trace!("handle cache stale");
                                self.map.remove(&key).unwrap();
                                ShardSelector::Known(shard_index)
                            }
                        },
                        None => ShardSelector::Known(shard_index),
                    }
                }
                RoutingResult::NeedConsultTenantManager => shard_selector,
            }
        };
        self.get_miss(timeline_id, miss, tenant_manager).await
    }

    #[inline(always)]
    fn shard_routing(&mut self, shard_selector: ShardSelector) -> RoutingResult<T> {
        loop {
            // terminates because when every iteration we remove an element from the map
            let Some((first_key, first_handle)) = self.map.iter().next() else {
                return RoutingResult::NeedConsultTenantManager;
            };
            let Some(first_handle) = first_handle.upgrade() else {
                // TODO: dedup with get()
                trace!("handle cache stale");
                let first_key_owned = *first_key;
                self.map.remove(&first_key_owned).unwrap();
                continue;
            };

            let first_handle_shard_identity = first_handle.timeline.get_shard_identity();
            let make_shard_index = |shard_num: ShardNumber| ShardIndex {
                shard_number: shard_num,
                shard_count: first_handle_shard_identity.count,
            };

            let idx = match shard_selector {
                ShardSelector::Page(key) => {
                    make_shard_index(first_handle_shard_identity.get_shard_number(&key))
                }
                ShardSelector::Zero => make_shard_index(ShardNumber(0)),
                ShardSelector::Known(shard_idx) => shard_idx,
            };

            if first_key.shard_index == idx {
                return RoutingResult::FastPath(Handle(first_handle));
            } else {
                return RoutingResult::Index(idx);
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    #[inline(always)]
    async fn get_miss(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        match tenant_manager.resolve(timeline_id, shard_selector).await {
            Ok(timeline) => {
                let key = timeline.shard_timeline_id();
                match &shard_selector {
                    ShardSelector::Zero => assert_eq!(key.shard_index.shard_number, ShardNumber(0)),
                    ShardSelector::Page(_) => (), // gotta trust tenant_manager
                    ShardSelector::Known(idx) => assert_eq!(idx, &key.shard_index),
                }

                let gate_guard = match timeline.gate().enter() {
                    Ok(guard) => guard,
                    Err(_) => {
                        return Err(GetError::TimelineGateClosed);
                    }
                };
                let handle = Arc::new(
                    // TODO: global metric that keeps track of the number of live HandlerTimeline instances
                    // so we can identify reference cycle bugs.
                    HandleInner {
                        shut_down: AtomicBool::new(false),
                        _gate_guard: gate_guard,
                        timeline: timeline.clone(),
                    },
                );
                let handle = {
                    let mut lock_guard = timeline
                        .per_timeline_state()
                        .handles
                        .lock()
                        .expect("mutex poisoned");
                    match &mut *lock_guard {
                        Some(timeline_handlers_list) => {
                            for handler in timeline_handlers_list.iter() {
                                assert!(!Arc::ptr_eq(handler, &handle));
                            }
                            timeline_handlers_list.push(Arc::clone(&handle));
                            match self.map.entry(key) {
                                hash_map::Entry::Occupied(mut o) => {
                                    // This should not happen in practice because the page_service
                                    // isn't calling get() concurrently(). Yet, let's deal with
                                    // this condition here so this module is a truly generic cache.
                                    if let Some(existing) = o.get().upgrade() {
                                        // Reuse to minimize the number of handles per timeline that are alive in the system.
                                        existing
                                    } else {
                                        o.insert(Arc::downgrade(&handle));
                                        handle
                                    }
                                }
                                hash_map::Entry::Vacant(v) => {
                                    v.insert(Arc::downgrade(&handle));
                                    handle
                                }
                            }
                        }
                        None => {
                            return Err(GetError::PerTimelineStateShutDown);
                        }
                    }
                };
                Ok(Handle(handle))
            }
            Err(e) => Err(GetError::TenantManager(e)),
        }
    }
}

impl<T: Types> PerTimelineState<T> {
    /// After this method returns, [`Cache::get`] will never again return a [`Handle`]
    /// to this Timeline object, even if [`TenantManager::resolve`] would still resolve
    /// to this Timeline object.
    ///
    /// Already-alive [`Handle`]s for this Timeline will remain open
    /// and keep the `Arc<Timeline>` allocation alive.
    /// The expectation is that
    /// 1. [`Handle`]s are short-lived (single call to [`Timeline`] method) and
    /// 2. [`Timeline`] methods invoked through [`Handle`] are sensitive
    ///    to [`Timeline::cancel`] and [`Timeline::is_stopping`].
    /// Thus, the [`Arc<Timeline>`] allocation's lifetime will only be minimally
    /// extended by the already-alive [`Handle`]s.
    #[instrument(level = "trace", skip_all)]
    pub(super) fn shutdown(&self) {
        let handles = self
            .handles
            .lock()
            .expect("mutex poisoned")
            // NB: this .take() sets locked to None.
            // That's what makes future `Cache::get` misses fail.
            // Cache hits are taken care of below.
            .take();
        let Some(handles) = handles else {
            trace!("already shut down");
            return;
        };
        for handle in handles {
            // Make hits fail.
            handle.shut_down.store(true, Ordering::Relaxed);
        }
    }
}

impl<T: Types> std::ops::Deref for Handle<T> {
    type Target = T::Timeline;
    fn deref(&self) -> &Self::Target {
        &self.0.timeline
    }
}
