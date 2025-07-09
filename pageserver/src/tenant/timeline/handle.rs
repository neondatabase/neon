//! A cache for [`crate::tenant::mgr`]+`Tenant::get_timeline`+`Timeline::gate.enter()`.
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
//! We want to avoid the overhead of doing, for each incoming request,
//! - tenant manager lookup (global rwlock + btreemap lookup for shard routing)
//! - cloning the `Arc<Timeline>` out of the tenant manager so we can
//!   release the mgr rwlock before doing any request processing work
//! - re-entering the Timeline gate for each Timeline method invocation.
//!
//! Regardless of how we accomplish the above, it should not
//! prevent the Timeline from shutting down promptly.
//!
//!
//! # Design
//!
//! ## Data Structures
//!
//! There are two concepts expressed as associated types in the `Types` trait:
//! - `TenantManager`: the thing that performs the expensive work. It produces
//!   a `Timeline` object, which is the other associated type.
//! - `Timeline`: the item that we cache for fast (TenantTimelineId,ShardSelector) lookup.
//!
//! There are three user-facing data structures exposed by this module:
//! - `PerTimelineState`: a struct embedded into each Timeline struct. Lifetime == Timeline lifetime.
//! - `Cache`: a struct private to each connection handler; Lifetime == connection lifetime.
//! - `Handle`: a smart pointer that derefs to the Types::Timeline.
//! - `WeakHandle`: downgrade of a `Handle` that does not keep the gate open, but allows
//!   trying to ugprade back to a `Handle`. If successful, a re-upgraded Handle will always
//!   point to the same cached `Types::Timeline`. Upgrades never invoke the `TenantManager`.
//!
//! Internally, there is 0 or 1 `HandleInner` per `(Cache,Timeline)`.
//! Since Cache:Connection is 1:1, there is 0 or 1 `HandleInner` per `(Connection,Timeline)`.
//!
//! The `HandleInner`  is allocated as a `Arc<Mutex<HandleInner>>` and
//! referenced weakly and strongly from various places which we are now illustrating.
//! For brevity, we will omit the `Arc<Mutex<>>` part in the following and instead
//! use `strong ref` and `weak ref` when referring to the `Arc<Mutex<HandleInner>>`
//! or `Weak<Mutex<HandleInner>>`, respectively.
//!
//! - The `Handle` is a strong ref.
//! - The `WeakHandle` is a weak ref.
//! - The `PerTimelineState` contains a `HashMap<CacheId, strong ref>`.
//! - The `Cache` is a `HashMap<unique identifier for the shard, weak ref>`.
//!
//! Lifetimes:
//! - `WeakHandle` and `Handle`: single pagestream request.
//! - `Cache`: single page service connection.
//! - `PerTimelineState`:  lifetime of the Timeline object (i.e., i.e., till `Timeline::shutdown`).
//!
//! ## Request Handling Flow (= filling and using the `Cache``)
//!
//! To dispatch a request, the page service connection calls `Cache::get`.
//!
//! A cache miss means we call Types::TenantManager::resolve for shard routing,
//! cloning the `Arc<Timeline>` out of it, and entering the gate. The result of
//! resolve() is the object we want to cache, and return `Handle`s to for subseqent `Cache::get` calls.
//!
//! We wrap the object returned from resolve() in an `Arc` and store that inside the
//! `Arc<Mutex<HandleInner>>>`. A weak ref to the HandleInner is stored in the `Cache`
//! and a strong ref in the `PerTimelineState`.
//! Another strong ref is returned wrapped in a `Handle`.
//!
//! For subsequent requests, `Cache::get` will perform a "fast path" shard routing
//! and find the weak ref in the cache.
//! We upgrade the weak ref to a strong ref and return it wrapped in a `Handle`.
//!
//! The pagestream processing is pipelined and involves a batching step.
//! While a request is batching, the `Handle` is downgraded to a `WeakHandle`.
//! When the batch is ready to be executed, the `WeakHandle` is upgraded back to a `Handle`
//! and the request handler dispatches the request to the right `<Handle as Deref<Target = Timeline>>::$request_method`.
//! It then drops the `Handle`, and thus the `Arc<Mutex<HandleInner>>` inside it.
//!
//! # Performance
//!
//! Remember from the introductory section:
//!
//! > We want to avoid the overhead of doing, for each incoming request,
//! > - tenant manager lookup (global rwlock + btreemap lookup for shard routing)
//! > - cloning the `Arc<Timeline>` out of the tenant manager so we can
//! >   release the mgr rwlock before doing any request processing work
//! > - re-entering the Timeline gate for each Timeline method invocation.
//!
//! All of these boil down to some state that is either globally shared among all shards
//! or state shared among all tasks that serve a particular timeline.
//! It is either protected by RwLock or manipulated via atomics.
//! Even atomics are costly when shared across multiple cores.
//! So, we want to avoid any permanent need for coordination between page_service tasks.
//!
//! The solution is to add indirection: we wrap the Types::Timeline object that is
//! returned by Types::TenantManager into an Arc that is rivate to the `HandleInner`
//! and hence to the single Cache / page_service connection.
//! (Review the "Data Structures" section if that is unclear to you.)
//!
//!
//! When upgrading a `WeakHandle`, we upgrade its weak to a strong ref (of the `Mutex<HandleInner>`),
//! lock the mutex, take out a clone of the `Arc<Types::Timeline>`, and drop the Mutex.
//! The Mutex is not contended because it is private to the connection.
//! And again, the  `Arc<Types::Timeline>` clone is cheap because that wrapper
//! Arc's refcounts are private to the connection.
//!
//! Downgrading drops these two Arcs, which again, manipulates refcounts that are private to the connection.
//!
//!
//! # Shutdown
//!
//! The attentive reader may have noticed the following reference cycle around the `Arc<Timeline>`:
//!
//! ```text
//! Timeline --owns--> PerTimelineState --strong--> HandleInner --strong--> Types::Timeline --strong--> Timeline
//! ```
//!
//! Further, there is this cycle:
//!
//! ```text
//! Timeline --owns--> PerTimelineState --strong--> HandleInner --strong--> Types::Timeline --strong--> GateGuard --keepalive--> Timeline
//! ```
//!
//! The former cycle is a memory leak if not broken.
//! The latter cycle further prevents the Timeline from shutting down
//! because we certainly won't drop the Timeline while the GateGuard is alive.
//! Preventing shutdown is the whole point of this handle/cache system,
//! but when the Timeline needs to shut down, we need to break the cycle.
//!
//! The cycle is broken by either
//! - Timeline shutdown (=> `PerTimelineState::shutdown`)
//! - Connection shutdown (=> dropping the `Cache`).
//!
//! Both transition the `HandleInner` from [`HandleInner::Open`] to
//! [`HandleInner::ShutDown`], which drops the only long-lived
//! `Arc<Types::Timeline>`. Once the last short-lived Arc<Types::Timeline>
//! is dropped, the `Types::Timeline` gets dropped and thereby
//! the `GateGuard` and the `Arc<Timeline>` that it stores,
//! thereby breaking both cycles.
//!
//! `PerTimelineState::shutdown` drops all the `HandleInners` it contains,
//! thereby breaking the cycle.
//! It also initiates draining of already existing `Handle`s by
//! poisoning things so that no new `HandleInner`'s can be added
//! to the `PerTimelineState`, which will make subsequent `Cache::get` fail.
//!
//! Concurrently existing / already upgraded `Handle`s will extend the
//! lifetime of the `Arc<Mutex<HandleInner>>` and hence cycles.
//! However, since `Handle`s are short-lived and new `Handle`s are not
//! handed out from `Cache::get` or `WeakHandle::upgrade` after
//! `PerTimelineState::shutdown`, that extension of the cycle is bounded.
//!
//! Concurrently existing `WeakHandle`s will fail to `upgrade()`:
//! while they will succeed in upgrading `Weak<Mutex<HandleInner>>`,
//! they will find the inner in state `HandleInner::ShutDown` state where the
//! `Arc<GateGuard>` and Timeline has already been dropped.
//!
//! Dropping the `Cache` undoes the registration of this `Cache`'s
//! `HandleInner`s from all the `PerTimelineState`s, i.e., it
//! removes the strong ref to each of its `HandleInner`s
//! from all the `PerTimelineState`.
//!
//! # Locking Rules
//!
//! To prevent deadlocks we:
//!
//! 1. Only ever hold one of the locks at a time.
//! 2. Don't add more than one Drop impl that locks on the
//!    cycles above.
//!
//! As per (2), that impl is in `Drop for Cache`.
//!
//! # Fast Path for Shard Routing
//!
//! The `Cache` has a fast path for shard routing to avoid calling into
//! the tenant manager for every request.
//!
//! The `Cache` maintains a hash map of `ShardTimelineId` to `WeakHandle`s.
//!
//! The current implementation uses the first entry in the hash map
//! to determine the `ShardParameters` and derive the correct
//! `ShardIndex` for the requested key.
//!
//! It then looks up the hash map for that `ShardTimelineId := {ShardIndex,TimelineId}`.
//!
//! If the lookup is successful and the `WeakHandle` can be upgraded,
//! it's a hit.
//!
//! ## Cache invalidation
//!
//! The insight is that cache invalidation is sufficient and most efficiently if done lazily.
//! The only reasons why an entry in the cache can become stale are:
//! 1. The `PerTimelineState` / Timeline is shutting down e.g. because the shard is
//!    being detached, timeline or shard deleted, or pageserver is shutting down.
//! 2. We're doing a shard split and new traffic should be routed to the child shards.
//!
//! Regarding (1), we will eventually fail to upgrade the `WeakHandle` once the
//! timeline has shut down, and when that happens, we remove the entry from the cache.
//!
//! Regarding (2), the insight is that it is toally fine to keep dispatching requests
//! to the parent shard during a shard split. Eventually, the shard split task will
//! shut down the parent => case (1).

use std::collections::HashMap;
use std::collections::hash_map;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use pageserver_api::shard::ShardIdentity;
use tracing::{instrument, trace};
use utils::id::TimelineId;
use utils::shard::{ShardIndex, ShardNumber};

use crate::tenant::mgr::ShardSelector;

/// The requirement for Debug is so that #[derive(Debug)] works in some places.
pub(crate) trait Types: Sized + std::fmt::Debug {
    type TenantManagerError: Sized + std::fmt::Debug;
    type TenantManager: TenantManager<Self> + Sized;
    type Timeline: Timeline<Self> + Sized;
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

type Map<T> = HashMap<ShardTimelineId, WeakHandle<T>>;

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
pub(crate) struct Handle<T: Types> {
    inner: Arc<Mutex<HandleInner<T>>>,
    open: Arc<T::Timeline>,
}
pub(crate) struct WeakHandle<T: Types> {
    inner: Weak<Mutex<HandleInner<T>>>,
}

enum HandleInner<T: Types> {
    Open(Arc<T::Timeline>),
    ShutDown,
}

/// Embedded in each [`Types::Timeline`] as the anchor for the only long-lived strong ref to `HandleInner`.
///
/// See module-level comment for details.
pub struct PerTimelineState<T: Types> {
    // None = shutting down
    #[allow(clippy::type_complexity)]
    handles: Mutex<Option<HashMap<CacheId, Arc<Mutex<HandleInner<T>>>>>>,
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
pub(crate) trait Timeline<T: Types> {
    fn shard_timeline_id(&self) -> ShardTimelineId;
    fn get_shard_identity(&self) -> &ShardIdentity;
    fn per_timeline_state(&self) -> &PerTimelineState<T>;
}

/// Errors returned by [`Cache::get`].
#[derive(Debug)]
pub(crate) enum GetError<T: Types> {
    TenantManager(T::TenantManagerError),
    PerTimelineStateShutDown,
}

/// Internal type used in [`Cache::get`].
enum RoutingResult<T: Types> {
    FastPath(Handle<T>),
    SlowPath(ShardTimelineId),
    NeedConsultTenantManager,
}

impl<T: Types> Cache<T> {
    /* BEGIN_HADRON */
    /// A wrapper of do_get to resolve the tenant shard for a get page request.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn get(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        const GET_MAX_RETRIES: usize = 10;
        const RETRY_BACKOFF: Duration = Duration::from_millis(100);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self
                .do_get(timeline_id, shard_selector, tenant_manager)
                .await
            {
                Ok(handle) => return Ok(handle),
                Err(e) => {
                    // Retry on tenant manager error to handle tenant split more gracefully
                    if attempt < GET_MAX_RETRIES {
                        tracing::warn!(
                            "Fail to resolve tenant shard in attempt {}: {:?}. Retrying...",
                            attempt,
                            e
                        );
                        tokio::time::sleep(RETRY_BACKOFF).await;
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
    /* END_HADRON */

    /// See module-level comment for details.
    ///
    /// Does NOT check for the shutdown state of [`Types::Timeline`].
    /// Instead, the methods of [`Types::Timeline`] that are invoked through
    /// the [`Handle`] are responsible for checking these conditions
    /// and if so, return an error that causes the page service to
    /// close the connection.
    #[instrument(level = "trace", skip_all)]
    async fn do_get(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        // terminates because when every iteration we remove an element from the map
        let miss: ShardSelector = loop {
            let routing_state = self.shard_routing(timeline_id, shard_selector);
            match routing_state {
                RoutingResult::FastPath(handle) => return Ok(handle),
                RoutingResult::SlowPath(key) => match self.map.get(&key) {
                    Some(cached) => match cached.upgrade() {
                        Ok(upgraded) => return Ok(upgraded),
                        Err(HandleUpgradeError::ShutDown) => {
                            // TODO: dedup with shard_routing()
                            trace!("handle cache stale");
                            self.map.remove(&key).unwrap();
                            continue;
                        }
                    },
                    None => break ShardSelector::Known(key.shard_index),
                },
                RoutingResult::NeedConsultTenantManager => break shard_selector,
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
            let Ok(first_handle) = first_handle.upgrade() else {
                // TODO: dedup with get()
                trace!("handle cache stale");
                let first_key_owned = *first_key;
                self.map.remove(&first_key_owned).unwrap();
                continue;
            };

            let first_handle_shard_identity = first_handle.get_shard_identity();
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
                timeline_id: first_handle.shard_timeline_id().timeline_id,
            };

            if need_shard_timeline_id == first_handle_shard_timeline_id {
                return RoutingResult::FastPath(first_handle);
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

                trace!("creating new HandleInner");
                let timeline = Arc::new(timeline);
                let handle_inner_arc =
                    Arc::new(Mutex::new(HandleInner::Open(Arc::clone(&timeline))));
                let handle_weak = WeakHandle {
                    inner: Arc::downgrade(&handle_inner_arc),
                };
                let handle = handle_weak
                    .upgrade()
                    .ok()
                    .expect("we just created it and it's not linked anywhere yet");
                {
                    let mut lock_guard = timeline
                        .per_timeline_state()
                        .handles
                        .lock()
                        .expect("mutex poisoned");
                    match &mut *lock_guard {
                        Some(per_timeline_state) => {
                            let replaced =
                                per_timeline_state.insert(self.id, Arc::clone(&handle_inner_arc));
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
                                    v.insert(handle_weak);
                                }
                            }
                        }
                        None => {
                            return Err(GetError::PerTimelineStateShutDown);
                        }
                    }
                }
                Ok(handle)
            }
            Err(e) => Err(GetError::TenantManager(e)),
        }
    }
}

pub(crate) enum HandleUpgradeError {
    ShutDown,
}

impl<T: Types> WeakHandle<T> {
    pub(crate) fn upgrade(&self) -> Result<Handle<T>, HandleUpgradeError> {
        let Some(inner) = Weak::upgrade(&self.inner) else {
            return Err(HandleUpgradeError::ShutDown);
        };
        let lock_guard = inner.lock().expect("poisoned");
        match &*lock_guard {
            HandleInner::Open(open) => {
                let open = Arc::clone(open);
                drop(lock_guard);
                Ok(Handle { open, inner })
            }
            HandleInner::ShutDown => Err(HandleUpgradeError::ShutDown),
        }
    }

    pub(crate) fn is_same_handle_as(&self, other: &WeakHandle<T>) -> bool {
        Weak::ptr_eq(&self.inner, &other.inner)
    }
}

impl<T: Types> std::ops::Deref for Handle<T> {
    type Target = T::Timeline;
    fn deref(&self) -> &Self::Target {
        &self.open
    }
}

impl<T: Types> Handle<T> {
    pub(crate) fn downgrade(&self) -> WeakHandle<T> {
        WeakHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl<T: Types> PerTimelineState<T> {
    /// After this method returns, [`Cache::get`] will never again return a [`Handle`]
    /// to the [`Types::Timeline`] that embeds this per-timeline state.
    /// Even if [`TenantManager::resolve`] would still resolve to it.
    ///
    /// Already-alive [`Handle`]s for will remain open, usable, and keeping the [`Types::Timeline`] alive.
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
        for handle_inner_arc in handles.values() {
            // Make hits fail.
            let mut lock_guard = handle_inner_arc.lock().expect("poisoned");
            lock_guard.shutdown();
        }
        drop(handles);
    }
}

// When dropping a [`Cache`], prune its handles in the [`PerTimelineState`] to break the reference cycle.
impl<T: Types> Drop for Cache<T> {
    fn drop(&mut self) {
        for (
            _,
            WeakHandle {
                inner: handle_inner_weak,
            },
        ) in self.map.drain()
        {
            let Some(handle_inner_arc) = handle_inner_weak.upgrade() else {
                continue;
            };
            let Some(handle_timeline) = handle_inner_arc
                // locking rules: drop lock before acquiring other lock below
                .lock()
                .expect("poisoned")
                .shutdown()
            else {
                // Concurrent PerTimelineState::shutdown.
                continue;
            };
            // Clean up per_timeline_state so the HandleInner allocation can be dropped.
            let per_timeline_state = handle_timeline.per_timeline_state();
            let mut handles_lock_guard = per_timeline_state.handles.lock().expect("mutex poisoned");
            let Some(handles) = &mut *handles_lock_guard else {
                continue;
            };
            let Some(removed_handle_inner_arc) = handles.remove(&self.id) else {
                // Concurrent PerTimelineState::shutdown.
                continue;
            };
            drop(handles_lock_guard); // locking rules!
            assert!(Arc::ptr_eq(&removed_handle_inner_arc, &handle_inner_arc));
        }
    }
}

impl<T: Types> HandleInner<T> {
    fn shutdown(&mut self) -> Option<Arc<T::Timeline>> {
        match std::mem::replace(self, HandleInner::ShutDown) {
            HandleInner::Open(timeline) => Some(timeline),
            HandleInner::ShutDown => {
                // Duplicate shutdowns are possible because both Cache::drop and PerTimelineState::shutdown
                // may do it concurrently, but locking rules disallow holding per-timeline-state lock and
                // the handle lock at the same time.
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use pageserver_api::key::{DBDIR_KEY, Key, rel_block_to_key};
    use pageserver_api::models::ShardParameters;
    use pageserver_api::reltag::RelTag;
    use pageserver_api::shard::ShardStripeSize;
    use utils::shard::ShardCount;
    use utils::sync::gate::GateGuard;

    use super::*;

    const FOREVER: std::time::Duration = std::time::Duration::from_secs(u64::MAX);

    #[derive(Debug)]
    struct TestTypes;
    impl Types for TestTypes {
        type TenantManagerError = anyhow::Error;
        type TenantManager = StubManager;
        type Timeline = Entered;
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

    struct Entered {
        timeline: Arc<StubTimeline>,
        #[allow(dead_code)] // it's stored here to keep the gate open
        gate_guard: Arc<GateGuard>,
    }

    impl StubTimeline {
        fn getpage(&self) {
            // do nothing
        }
    }

    impl Timeline<TestTypes> for Entered {
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
        ) -> anyhow::Result<Entered> {
            for timeline in &self.shards {
                if timeline.id == timeline_id {
                    let enter_gate = || {
                        let gate_guard = timeline.gate.enter()?;
                        let gate_guard = Arc::new(gate_guard);
                        anyhow::Ok(gate_guard)
                    };
                    match &shard_selector {
                        ShardSelector::Zero if timeline.shard.is_shard_zero() => {
                            return Ok(Entered {
                                timeline: Arc::clone(timeline),
                                gate_guard: enter_gate()?,
                            });
                        }
                        ShardSelector::Zero => continue,
                        ShardSelector::Page(key) if timeline.shard.is_key_local(key) => {
                            return Ok(Entered {
                                timeline: Arc::clone(timeline),
                                gate_guard: enter_gate()?,
                            });
                        }
                        ShardSelector::Page(_) => continue,
                        ShardSelector::Known(idx) if idx == &timeline.shard.shard_index() => {
                            return Ok(Entered {
                                timeline: Arc::clone(timeline),
                                gate_guard: enter_gate()?,
                            });
                        }
                        ShardSelector::Known(_) => continue,
                    }
                }
            }
            anyhow::bail!("not found")
        }
    }

    impl std::ops::Deref for Entered {
        type Target = StubTimeline;
        fn deref(&self) -> &Self::Target {
            &self.timeline
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
        let handle: Handle<_> = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));
        assert_eq!(cache.map.len(), 1);
        drop(handle);

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
            cache.map.len(),
            1,
            "this is an implementation detail but worth pointing out: we can't clear the cache from shutdown(), it's cleared on first access after"
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
        drop(shard0);
        drop(mgr);
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

    fn make_relation_key_for_shard(shard: ShardNumber, params: ShardParameters) -> Key {
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
            shard: ShardIdentity::from_params(ShardNumber(0), child_params),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child1 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::from_params(ShardNumber(1), child_params),
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
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), child_params)),
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
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), child_params)),
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
                ShardSelector::Page(make_relation_key_for_shard(ShardNumber(0), child_params)),
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
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), child_params)),
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
        }

        // No handles exist, thus gates are closed and don't require shutdown.
        // Thus the gate should close immediately, even without shutdown.
        tokio::select! {
            _ = shard0.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("handle is dropped, no other gate holders exist")
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_weak_handles() {
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

        let refcount_start = Arc::strong_count(&shard0);

        let key = DBDIR_KEY;

        let mut cache = Cache::<TestTypes>::default();

        let handle = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));

        let weak_handle = handle.downgrade();

        drop(handle);

        let upgraded_handle = weak_handle.upgrade().ok().expect("we can upgrade it");

        // Start shutdown
        shard0.per_timeline_state.shutdown();

        // Upgrades during shutdown don't work, even if upgraded_handle exists.
        weak_handle
            .upgrade()
            .err()
            .expect("can't upgrade weak handle as soon as shutdown started");

        // But upgraded_handle is still alive, so the gate won't close.
        tokio::select! {
            _ = shard0.gate.close() => {
                panic!("handle is keeping gate open");
            }
            _ = tokio::time::sleep(FOREVER) => { }
        }

        // Drop the last handle.
        drop(upgraded_handle);

        // The gate should close now, despite there still being a weak_handle.
        tokio::select! {
            _ = shard0.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("only strong handle is dropped and we shut down per-timeline-state")
            }
        }

        // The weak handle still can't be upgraded.
        weak_handle
            .upgrade()
            .err()
            .expect("still shouldn't be able to upgrade the weak handle");

        // There should be no strong references to the timeline object except the one on "stack".
        assert_eq!(Arc::strong_count(&shard0), refcount_start);
    }

    #[tokio::test(start_paused = true)]
    async fn test_reference_cycle_broken_when_cache_is_dropped() {
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

        // helper to check if a handle is referenced by per_timeline_state
        let per_timeline_state_refs_handle = |handle_weak: &Weak<Mutex<HandleInner<_>>>| {
            let per_timeline_state = shard0.per_timeline_state.handles.lock().unwrap();
            let per_timeline_state = per_timeline_state.as_ref().unwrap();
            per_timeline_state
                .values()
                .any(|v| Weak::ptr_eq(&Arc::downgrade(v), handle_weak))
        };

        // Fill the cache.
        let handle = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));
        let handle_inner_weak = Arc::downgrade(&handle.inner);
        assert!(
            per_timeline_state_refs_handle(&handle_inner_weak),
            "we still hold `handle` _and_ haven't dropped `cache` yet"
        );

        // Drop the cache.
        drop(cache);

        assert!(
            !(per_timeline_state_refs_handle(&handle_inner_weak)),
            "nothing should reference the handle allocation anymore"
        );
        assert!(
            Weak::upgrade(&handle_inner_weak).is_some(),
            "the local `handle` still keeps the allocation alive"
        );
        // but obviously the cache is gone so no new allocations can be handed out.

        // Drop handle.
        drop(handle);
        assert!(
            Weak::upgrade(&handle_inner_weak).is_none(),
            "the local `handle` is dropped, so the allocation should be dropped by now"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_reference_cycle_broken_when_per_timeline_state_shutdown() {
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
        let handle = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        // grab a weak reference to the inner so can later try to Weak::upgrade it and assert that fails
        let handle_inner_weak = Arc::downgrade(&handle.inner);

        // drop the handle, obviously the lifetime of `inner` is at least as long as each strong reference to it
        drop(handle);
        assert!(Weak::upgrade(&handle_inner_weak).is_some(), "can still");

        // Shutdown the per_timeline_state.
        shard0.per_timeline_state.shutdown();
        assert!(Weak::upgrade(&handle_inner_weak).is_none(), "can no longer");

        // cache only contains Weak's, so, it can outlive the per_timeline_state without
        // Drop explicitly solely to make this point.
        drop(cache);
    }
}
