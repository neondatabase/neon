//! An efficient way to keep the timeline gate open without preventing
//! timeline shutdown for longer than a single call to a timeline method.
//!
//! # Motivation
//!
//! On a single page service connection, we're typically serving a single TenantTimelineId.
//!
//! Without sharding, there is a single Timeline object to which we dispatch
//! all requests. For example, a getpage request gets dispatched to the
//! Timeline::get method of the Timeline object that represents the
//! (tenant,timeline) of that connection.
//!
//! With sharding, for each request that comes in on the connection,
//! we first have to perform shard routing based on the requested key (=~ page number).
//! The result of shard routing is a Timeline object.
//! We then dispatch the request to that Timeline object.
//!
//! Regardless of whether the tenant is sharded or not, we want to ensure that
//! we hold the Timeline gate open while we're invoking the method on the
//! Timeline object.
//!
//! However, we want to avoid the overhead of entering the gate for every
//! method invocation.
//!
//! Further, for shard routing, we want to avoid calling the tenant manager to
//! resolve the shard for every request. Instead, we want to cache the
//! routing result so we can bypass the tenant manager for all subsequent requests
//! that get routed to that shard.
//!
//! Regardless of how we accomplish the above, it should not
//! prevent the Timeline from shutting down promptly.
//!
//! # Design
//!
//! There are three user-facing data structures:
//! - `PerTimelineState`: a struct embedded into each Timeline struct. Lifetime == Timeline lifetime.
//! - `Cache`: a struct private to each connection handler; Lifetime == connection lifetime.
//! - `Handle`: a smart pointer that holds the Timeline gate open and derefs to `&Timeline`.
//!   Lifetime: for a single request dispatch on the Timeline (i.e., one getpage request)
//!
//! The `Handle` is just a wrapper around an `Arc<HandleInner>`.
//!
//! There is one long-lived `Arc<HandleInner>`, which is stored in the `PerTimelineState`.
//! The `Cache` stores a `Weak<HandleInner>` for each cached Timeline.
//!
//! To dispatch a request, the page service connection calls `Cache::get`.
//!
//! A cache miss means we consult the tenant manager for shard routing,
//! resulting in an `Arc<Timeline>`. We enter its gate _once_ and construct an
//! `Arc<HandleInner>`. We store a `Weak<HandleInner>` in the cache
//! and the `Arc<HandleInner>` in the `PerTimelineState`.
//!
//! For subsequent requests, `Cache::get` will perform a "fast path" shard routing
//! and find the `Weak<HandleInner>` in the cache.
//! We upgrade the `Weak<HandleInner>` to an `Arc<HandleInner>` and wrap it in the user-facing `Handle` type.
//!
//! The request handler dispatches the request to the right `<Handle as Deref<Target = Timeline>>::$request_method`.
//! It then drops the `Handle`, which drops the `Arc<HandleInner>`.
//!
//! # Memory Management / How The Reference Cycle Is Broken
//!
//! The attentive reader may have noticed the strong reference cycle
//! from `Arc<HandleInner>` to `PerTimelineState` to `Arc<Timeline>`.
//!
//! This cycle is intentional: while it exists, the `Cache` can upgrade its
//! `Weak<HandleInner>` to an `Arc<HandleInner>` in a single atomic operation.
//!
//! The cycle is broken by either
//! - `PerTimelineState::shutdown` or
//! - dropping the `Cache`.
//!
//! Concurrently existing `Handle`s will extend the existence of the cycle.
//! However, since `Handle`s are short-lived and new `Handle`s are not
//! handed out after either `PerTimelineState::shutdown` or `Cache` drop,
//! that extension of the cycle is bounded.
//!
//! # Fast Path for Shard Routing
//!
//! The `Cache` has a fast path for shard routing to avoid calling into
//! the tenant manager for every request.
//!
//! The `Cache` maintains a hash map of `ShardTimelineId` to `Weak<HandleInner>`.
//!
//! The current implementation uses the first entry in the hash map
//! to determine the `ShardParameters` and derive the correct
//! `ShardIndex` for the requested key.
//!
//! It then looks up the hash map for that `ShardTimelineId := {ShardIndex,TimelineId}`.
//!
//! If the lookup is successful and the `Weak<HandleInner>` can be upgraded,
//! it's a hit.
//!
//! ## Cache invalidation
//!
//! The insight is that cache invalidation is sufficient and most efficiently done lazily.
//! The only reasons why an entry in the cache can become stale are:
//! 1. The `PerTimelineState` / Timeline is shutting down e.g. because the shard is
//!    being detached, timeline or shard deleted, or pageserver is shutting down.
//! 2. We're doing a shard split and new traffic should be routed to the child shards.
//!
//! Regarding (1), we will eventually fail to upgrade the `Weak<HandleInner>` once the
//! timeline has shut down, and when that happens, we remove the entry from the cache.
//!
//! Regarding (2), the insight is that it is toally fine to keep dispatching requests
//! to the parent shard during a shard split. Eventually, the shard split task will
//! shut down the parent => case (1).

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

/// The requirement for Debug is so that #[derive(Debug)] works in some places.
pub(crate) trait Types: Sized + std::fmt::Debug {
    type TenantManagerError: Sized + std::fmt::Debug;
    type TenantManager: TenantManager<Self> + Sized;
    type Timeline: ArcTimeline<Self> + Sized;
}

/// Uniquely identifies a [`Cache`] instance over the lifetime of the process.
/// Required so [`Cache::drop`] can take out the handles from the [`PerTimelineState`].
/// Alternative to this would be to allocate [`Cache`] in a `Box` and identify it by the pointer.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
struct CacheId(u64);

impl CacheId {
    fn next() -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if id == 0 {
            panic!("CacheId::new() returned 0, overflow");
        }
        Self(id)
    }
}

/// See module-level comment.
pub(crate) struct Cache<T: Types> {
    id: CacheId,
    map: Map<T>,
}

type Map<T> = HashMap<ShardTimelineId, Weak<HandleInner<T>>>;

impl<T: Types> Default for Cache<T> {
    fn default() -> Self {
        Self {
            id: CacheId::next(),
            map: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone, Copy)]
pub(crate) struct ShardTimelineId {
    pub(crate) shard_index: ShardIndex,
    pub(crate) timeline_id: TimelineId,
}

/// See module-level comment.
pub(crate) struct Handle<T: Types>(Arc<HandleInner<T>>);
struct HandleInner<T: Types> {
    shut_down: AtomicBool,
    timeline: T::Timeline,
    // The timeline's gate held open.
    _gate_guard: utils::sync::gate::GateGuard,
}

/// Embedded in each [`Types::Timeline`] as the anchor for the only long-lived strong ref to `HandleInner`.
///
/// See module-level comment for details.
pub struct PerTimelineState<T: Types> {
    // None = shutting down
    handles: Mutex<Option<HashMap<CacheId, Arc<HandleInner<T>>>>>,
}

impl<T: Types> Default for PerTimelineState<T> {
    fn default() -> Self {
        Self {
            handles: Mutex::new(Some(Default::default())),
        }
    }
}

/// Abstract view of [`crate::tenant::mgr`], for testability.
pub(crate) trait TenantManager<T: Types> {
    /// Invoked by [`Cache::get`] to resolve a [`ShardTimelineId`] to a [`Types::Timeline`].
    /// Errors are returned as [`GetError::TenantManager`].
    async fn resolve(
        &self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> Result<T::Timeline, T::TenantManagerError>;
}

/// Abstract view of an [`Arc<Timeline>`], for testability.
pub(crate) trait ArcTimeline<T: Types>: Clone {
    fn gate(&self) -> &utils::sync::gate::Gate;
    fn shard_timeline_id(&self) -> ShardTimelineId;
    fn get_shard_identity(&self) -> &ShardIdentity;
    fn per_timeline_state(&self) -> &PerTimelineState<T>;
}

/// Errors returned by [`Cache::get`].
#[derive(Debug)]
pub(crate) enum GetError<T: Types> {
    TenantManager(T::TenantManagerError),
    TimelineGateClosed,
    PerTimelineStateShutDown,
}

/// Internal type used in [`Cache::get`].
enum RoutingResult<T: Types> {
    FastPath(Handle<T>),
    SlowPath(ShardTimelineId),
    NeedConsultTenantManager,
}

impl<T: Types> Cache<T> {
    /// See module-level comment for details.
    ///
    /// Does NOT check for the shutdown state of [`Types::Timeline`].
    /// Instead, the methods of [`Types::Timeline`] that are invoked through
    /// the [`Handle`] are responsible for checking these conditions
    /// and if so, return an error that causes the page service to
    /// close the connection.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn get(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        // terminates because each iteration removes an element from the map
        loop {
            let handle = self
                .get_impl(timeline_id, shard_selector, tenant_manager)
                .await?;
            if handle.0.shut_down.load(Ordering::Relaxed) {
                let removed = self
                    .map
                    .remove(&handle.0.timeline.shard_timeline_id())
                    .expect("invariant of get_impl is that the returned handle is in the map");
                assert!(
                    Weak::ptr_eq(&removed, &Arc::downgrade(&handle.0)),
                    "shard_timeline_id() incorrect?"
                );
            } else {
                return Ok(handle);
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn get_impl(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        let miss: ShardSelector = {
            let routing_state = self.shard_routing(timeline_id, shard_selector);
            match routing_state {
                RoutingResult::FastPath(handle) => return Ok(handle),
                RoutingResult::SlowPath(key) => match self.map.get(&key) {
                    Some(cached) => match cached.upgrade() {
                        Some(upgraded) => return Ok(Handle(upgraded)),
                        None => {
                            trace!("handle cache stale");
                            self.map.remove(&key).unwrap();
                            ShardSelector::Known(key.shard_index)
                        }
                    },
                    None => ShardSelector::Known(key.shard_index),
                },
                RoutingResult::NeedConsultTenantManager => shard_selector,
            }
        };
        self.get_miss(timeline_id, miss, tenant_manager).await
    }

    #[inline(always)]
    fn shard_routing(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> RoutingResult<T> {
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

            let need_idx = match shard_selector {
                ShardSelector::Page(key) => {
                    make_shard_index(first_handle_shard_identity.get_shard_number(&key))
                }
                ShardSelector::Zero => make_shard_index(ShardNumber(0)),
                ShardSelector::Known(shard_idx) => shard_idx,
            };
            let need_shard_timeline_id = ShardTimelineId {
                shard_index: need_idx,
                timeline_id,
            };
            let first_handle_shard_timeline_id = ShardTimelineId {
                shard_index: first_handle_shard_identity.shard_index(),
                timeline_id: first_handle.timeline.shard_timeline_id().timeline_id,
            };

            if need_shard_timeline_id == first_handle_shard_timeline_id {
                return RoutingResult::FastPath(Handle(first_handle));
            } else {
                return RoutingResult::SlowPath(need_shard_timeline_id);
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
                trace!("creating new HandleInner");
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
                        Some(per_timeline_state) => {
                            let replaced = per_timeline_state.insert(self.id, Arc::clone(&handle));
                            assert!(replaced.is_none(), "some earlier code left a stale handle");
                            match self.map.entry(key) {
                                hash_map::Entry::Occupied(_o) => {
                                    // This cannot not happen because
                                    // 1. we're the _miss_ handle, i.e., `self.map` didn't contain an entry and
                                    // 2. we were holding &mut self during .resolve().await above, so, no other thread can have inserted a handle
                                    //    while we were waiting for the tenant manager.
                                    unreachable!()
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
    /// to the [`Types::Timeline`] that embeds this per-timeline state.
    /// Even if [`TenantManager::resolve`] would still resolve to it.
    ///
    /// Already-alive [`Handle`]s for will remain open, usable, and keeping the [`ArcTimeline`] alive.
    /// That's ok because they're short-lived. See module-level comment for details.
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
        for handle in handles.values() {
            // Make hits fail.
            handle.shut_down.store(true, Ordering::Relaxed);
        }
        drop(handles);
    }
}

impl<T: Types> std::ops::Deref for Handle<T> {
    type Target = T::Timeline;
    fn deref(&self) -> &Self::Target {
        &self.0.timeline
    }
}

#[cfg(test)]
impl<T: Types> Drop for HandleInner<T> {
    fn drop(&mut self) {
        trace!("HandleInner dropped");
    }
}

// When dropping a [`Cache`], prune its handles in the [`PerTimelineState`] to break the reference cycle.
impl<T: Types> Drop for Cache<T> {
    fn drop(&mut self) {
        for (_, weak) in self.map.drain() {
            if let Some(strong) = weak.upgrade() {
                // handle is still being kept alive in PerTimelineState
                let timeline = strong.timeline.per_timeline_state();
                let mut handles = timeline.handles.lock().expect("mutex poisoned");
                if let Some(handles) = &mut *handles {
                    let Some(removed) = handles.remove(&self.id) else {
                        // There could have been a shutdown inbetween us upgrading the weak and locking the mutex.
                        continue;
                    };
                    assert!(Arc::ptr_eq(&removed, &strong));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pageserver_api::{
        key::{rel_block_to_key, Key, DBDIR_KEY},
        models::ShardParameters,
        reltag::RelTag,
        shard::ShardStripeSize,
    };
    use utils::shard::ShardCount;

    use super::*;

    const FOREVER: std::time::Duration = std::time::Duration::from_secs(u64::MAX);

    #[derive(Debug)]
    struct TestTypes;
    impl Types for TestTypes {
        type TenantManagerError = anyhow::Error;
        type TenantManager = StubManager;
        type Timeline = Arc<StubTimeline>;
    }

    struct StubManager {
        shards: Vec<Arc<StubTimeline>>,
    }

    struct StubTimeline {
        gate: utils::sync::gate::Gate,
        id: TimelineId,
        shard: ShardIdentity,
        per_timeline_state: PerTimelineState<TestTypes>,
        myself: Weak<StubTimeline>,
    }

    impl StubTimeline {
        fn getpage(&self) {
            // do nothing
        }
    }

    impl ArcTimeline<TestTypes> for Arc<StubTimeline> {
        fn gate(&self) -> &utils::sync::gate::Gate {
            &self.gate
        }

        fn shard_timeline_id(&self) -> ShardTimelineId {
            ShardTimelineId {
                shard_index: self.shard.shard_index(),
                timeline_id: self.id,
            }
        }

        fn get_shard_identity(&self) -> &ShardIdentity {
            &self.shard
        }

        fn per_timeline_state(&self) -> &PerTimelineState<TestTypes> {
            &self.per_timeline_state
        }
    }

    impl TenantManager<TestTypes> for StubManager {
        async fn resolve(
            &self,
            timeline_id: TimelineId,
            shard_selector: ShardSelector,
        ) -> anyhow::Result<Arc<StubTimeline>> {
            for timeline in &self.shards {
                if timeline.id == timeline_id {
                    match &shard_selector {
                        ShardSelector::Zero if timeline.shard.is_shard_zero() => {
                            return Ok(Arc::clone(timeline));
                        }
                        ShardSelector::Zero => continue,
                        ShardSelector::Page(key) if timeline.shard.is_key_local(key) => {
                            return Ok(Arc::clone(timeline));
                        }
                        ShardSelector::Page(_) => continue,
                        ShardSelector::Known(idx) if idx == &timeline.shard.shard_index() => {
                            return Ok(Arc::clone(timeline));
                        }
                        ShardSelector::Known(_) => continue,
                    }
                }
            }
            anyhow::bail!("not found")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeline_shutdown() {
        crate::tenant::harness::setup_logging();

        let timeline_id = TimelineId::generate();
        let shard0 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let mgr = StubManager {
            shards: vec![shard0.clone()],
        };
        let key = DBDIR_KEY;

        let mut cache = Cache::<TestTypes>::default();

        //
        // fill the cache
        //
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (2, 1),
            "strong: shard0, mgr; weak: myself"
        );

        let handle: Handle<_> = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        let handle_inner_weak = Arc::downgrade(&handle.0);
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));
        assert_eq!(
            (
                Weak::strong_count(&handle_inner_weak),
                Weak::weak_count(&handle_inner_weak)
            ),
            (2, 2),
            "strong: handle, per_timeline_state, weak: handle_inner_weak, cache"
        );
        assert_eq!(cache.map.len(), 1);

        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (3, 1),
            "strong: handleinner(per_timeline_state), shard0, mgr; weak: myself"
        );
        drop(handle);
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (3, 1),
            "strong: handleinner(per_timeline_state), shard0, mgr; weak: myself"
        );

        //
        // demonstrate that Handle holds up gate closure
        // but shutdown prevents new handles from being handed out
        //

        tokio::select! {
            _ = shard0.gate.close() => {
                panic!("cache and per-timeline handler state keep cache open");
            }
            _ = tokio::time::sleep(FOREVER) => {
                // NB: first poll of close() makes it enter closing state
            }
        }

        let handle = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));

        // SHUTDOWN
        shard0.per_timeline_state.shutdown(); // keeping handle alive across shutdown

        assert_eq!(
            1,
            Weak::strong_count(&handle_inner_weak),
            "through local var handle"
        );
        assert_eq!(
            cache.map.len(),
            1,
            "this is an implementation detail but worth pointing out: we can't clear the cache from shutdown(), it's cleared on first access after"
        );
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (3, 1),
            "strong: handleinner(via handle), shard0, mgr; weak: myself"
        );

        // this handle is perfectly usable
        handle.getpage();

        cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .err()
            .expect("documented behavior: can't get new handle after shutdown, even if there is an alive Handle");
        assert_eq!(
            cache.map.len(),
            0,
            "first access after shutdown cleans up the Weak's from the cache"
        );

        tokio::select! {
            _ = shard0.gate.close() => {
                panic!("handle is keeping gate open");
            }
            _ = tokio::time::sleep(FOREVER) => { }
        }

        drop(handle);
        assert_eq!(
            0,
            Weak::strong_count(&handle_inner_weak),
            "the HandleInner destructor already ran"
        );
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (2, 1),
            "strong: shard0, mgr; weak: myself"
        );

        // closing gate succeeds after dropping handle
        tokio::select! {
            _ = shard0.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("handle is dropped, no other gate holders exist")
            }
        }

        // map gets cleaned on next lookup
        cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .err()
            .expect("documented behavior: can't get new handle after shutdown");
        assert_eq!(cache.map.len(), 0);

        // ensure all refs to shard0 are gone and we're not leaking anything
        let myself = Weak::clone(&shard0.myself);
        drop(shard0);
        drop(mgr);
        assert_eq!(Weak::strong_count(&myself), 0);
    }

    #[tokio::test]
    async fn test_multiple_timelines_and_deletion() {
        crate::tenant::harness::setup_logging();

        let timeline_a = TimelineId::generate();
        let timeline_b = TimelineId::generate();
        assert_ne!(timeline_a, timeline_b);
        let timeline_a = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_a,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let timeline_b = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_b,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let mut mgr = StubManager {
            shards: vec![timeline_a.clone(), timeline_b.clone()],
        };
        let key = DBDIR_KEY;

        let mut cache = Cache::<TestTypes>::default();

        cache
            .get(timeline_a.id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have it");
        cache
            .get(timeline_b.id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have it");
        assert_eq!(cache.map.len(), 2);

        // delete timeline A
        timeline_a.per_timeline_state.shutdown();
        mgr.shards.retain(|t| t.id != timeline_a.id);
        assert!(
            mgr.resolve(timeline_a.id, ShardSelector::Page(key))
                .await
                .is_err(),
            "broken StubManager implementation"
        );

        assert_eq!(
            cache.map.len(),
            2,
            "cache still has a Weak handle to Timeline A"
        );
        cache
            .get(timeline_a.id, ShardSelector::Page(key), &mgr)
            .await
            .err()
            .expect("documented behavior: can't get new handle after shutdown");
        assert_eq!(cache.map.len(), 1, "next access cleans up the cache");

        cache
            .get(timeline_b.id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we still have it");
    }

    fn make_relation_key_for_shard(shard: ShardNumber, params: &ShardParameters) -> Key {
        rel_block_to_key(
            RelTag {
                spcnode: 1663,
                dbnode: 208101,
                relnode: 2620,
                forknum: 0,
            },
            shard.0 as u32 * params.stripe_size.0,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_shard_split() {
        crate::tenant::harness::setup_logging();
        let timeline_id = TimelineId::generate();
        let parent = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child_params = ShardParameters {
            count: ShardCount(2),
            stripe_size: ShardStripeSize::default(),
        };
        let child0 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::from_params(ShardNumber(0), &child_params),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child1 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::from_params(ShardNumber(1), &child_params),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child_shards_by_shard_number = [child0.clone(), child1.clone()];

        let mut cache = Cache::<TestTypes>::default();

        // fill the cache with the parent
        for i in 0..2 {
            let handle = cache
                .get(
                    timeline_id,
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), &child_params)),
                    &StubManager {
                        shards: vec![parent.clone()],
                    },
                )
                .await
                .expect("we have it");
            assert!(
                Weak::ptr_eq(&handle.myself, &parent.myself),
                "mgr returns parent first"
            );
            drop(handle);
        }

        //
        // SHARD SPLIT: tenant manager changes, but the cache isn't informed
        //

        // while we haven't shut down the parent, the cache will return the cached parent, even
        // if the tenant manager returns the child
        for i in 0..2 {
            let handle = cache
                .get(
                    timeline_id,
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), &child_params)),
                    &StubManager {
                        shards: vec![], // doesn't matter what's in here, the cache is fully loaded
                    },
                )
                .await
                .expect("we have it");
            assert!(
                Weak::ptr_eq(&handle.myself, &parent.myself),
                "mgr returns parent"
            );
            drop(handle);
        }

        let parent_handle = cache
            .get(
                timeline_id,
                ShardSelector::Page(make_relation_key_for_shard(ShardNumber(0), &child_params)),
                &StubManager {
                    shards: vec![parent.clone()],
                },
            )
            .await
            .expect("we have it");
        assert!(Weak::ptr_eq(&parent_handle.myself, &parent.myself));

        // invalidate the cache
        parent.per_timeline_state.shutdown();

        // the cache will now return the child, even though the parent handle still exists
        for i in 0..2 {
            let handle = cache
                .get(
                    timeline_id,
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), &child_params)),
                    &StubManager {
                        shards: vec![child0.clone(), child1.clone()], // <====== this changed compared to previous loop
                    },
                )
                .await
                .expect("we have it");
            assert!(
                Weak::ptr_eq(
                    &handle.myself,
                    &child_shards_by_shard_number[i as usize].myself
                ),
                "mgr returns child"
            );
            drop(handle);
        }

        // all the while the parent handle kept the parent gate open
        tokio::select! {
            _ = parent_handle.gate.close() => {
                panic!("parent handle is keeping gate open");
            }
            _ = tokio::time::sleep(FOREVER) => { }
        }
        drop(parent_handle);
        tokio::select! {
            _ = parent.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("parent handle is dropped, no other gate holders exist")
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_connection_handler_exit() {
        crate::tenant::harness::setup_logging();
        let timeline_id = TimelineId::generate();
        let shard0 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let mgr = StubManager {
            shards: vec![shard0.clone()],
        };
        let key = DBDIR_KEY;

        // Simulate 10 connections that's opened, used, and closed
        let mut used_handles = vec![];
        for _ in 0..10 {
            let mut cache = Cache::<TestTypes>::default();
            let handle = {
                let handle = cache
                    .get(timeline_id, ShardSelector::Page(key), &mgr)
                    .await
                    .expect("we have the timeline");
                assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));
                handle
            };
            handle.getpage();
            used_handles.push(Arc::downgrade(&handle.0));
        }

        // No handles exist, thus gates are closed and don't require shutdown
        assert!(used_handles
            .iter()
            .all(|weak| Weak::strong_count(weak) == 0));

        // ... thus the gate should close immediately, even without shutdown
        tokio::select! {
            _ = shard0.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("handle is dropped, no other gate holders exist")
            }
        }
    }
}
