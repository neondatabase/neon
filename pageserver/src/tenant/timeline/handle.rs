use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use either::Either;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use utils::id::TenantId;
use utils::id::TenantShardTimelineId;
use utils::sync::gate::GateGuard;

use super::Timeline;

#[derive(Default)]
pub(super) struct Handles {
    // None = shutting down
    handlers: Mutex<Option<Vec<Arc<HandlerTimeline>>>>,
}

struct HandlerTimeline {
    _gate_guard: utils::sync::gate::GateGuard,
    timeline: Arc<Timeline>,
}

impl std::ops::Deref for HandlerTimeline {
    type Target = Timeline;
    fn deref(&self) -> &Self::Target {
        &self.timeline
    }
}

#[derive(Default)]
pub(crate) struct Cache {
    cache: HashMap<TenantShardTimelineId, Weak<HandlerTimeline>>,
}

pub(crate) enum LookupResult {
    Hit(Arc<HandlerTimeline>),
}

pub(crate) trait TenantManager {
    type Error;
    fn resolve(&self, key: &TenantShardTimelineId) -> Result<Arc<Timeline>, Self::Error>;
}

pub(crate) enum GetError<M> {
    TenantManager(M),
    TimelineGateClosed,
}

impl Cache {
    #[instrument(level = "debug", skip_all, fields(%key))]
    pub(crate) fn get<M, E>(
        &mut self,
        key: &TenantShardTimelineId,
        tenant_manager: M,
    ) -> Result<Arc<HandlerTimeline>, GetError<E>>
    where
        M: TenantManager<Error = E>,
    {
        let cached: Option<_> = match self.cache.get(&key) {
            None => {
                debug!("cache miss, never seen");
                None
            }
            Some(weak) => match weak.upgrade() {
                None => {
                    debug!("handle cache stale");
                    // clean up cache
                    self.cache.remove(&key).unwrap();
                    None
                }
                Some(timeline) => {
                    debug!("handle cache hit");
                    Some(timeline)
                }
            },
        };
        match cached {
            Some(timeline) => return Ok(timeline),
            None => match tenant_manager.resolve(key) {
                Ok(timeline) => {
                    let gate_guard = match timeline.gate.enter() {
                        Ok(guard) => guard,
                        Err(_) => {
                            return Err(GetError::TimelineGateClosed);
                        }
                    };
                    let handler_timeline = Arc::new(
                        // TODO: global metric that keeps track of the number of live HandlerTimeline instances
                        // so we can identify reference cycle bugs.
                        HandlerTimeline {
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
                                    assert!(!Arc::ptr_eq(handler, &handler_timeline));
                                }
                                timeline_handlers_list.push(Arc::clone(&handler_timeline));
                                self.cache.insert(*key, Arc::downgrade(&handler_timeline));
                            }
                            None => {
                                return Err(GetError::TimelineGateClosed);
                            }
                        }
                    }
                    return Ok(handler_timeline);
                }
                Err(e) => return Err(GetError::TenantManager(e)),
            },
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        for (_, cached) in self.cache.drain() {
            if let Some(cached) = cached.upgrade() {
                let mut lock_guard = cached
                    .timeline
                    .handlers
                    .handlers
                    .lock()
                    .expect("mutex poisoned");
                if let Some(handlers) = &mut *lock_guard {
                    handlers.retain(|handler| !Arc::ptr_eq(handler, &cached));
                }
            }
        }
    }
}
