//! # Memory Management
//!
//! Before Timeline shutdown we have a reference cycle
//! ```
//! Timeline::handlers =strong=> HandlerTimeline =strong=> Timeline
//! ```
//!
//! We do this so the hot path need only do one, uncontended, atomic increment, i.e.,
//! the Weak<HandlerTimeline>.
//!
//! Timeline::shutdown breaks the reference cycle.
//!
//! Conn shutdown removes the Arc<HandlerTimeline> from Timeline::handlers.

use tracing::{info, Instrument};

struct Timeline {
    tenant_id: TenantId,
    shard_id: ShardId,
    timeline_id: TimelineId,

    gate: utils::sync::gate::Gate,
    // None = shutting down
    handlers: Mutex<Option<Vec<Arc<HandlerTimeline>>>>,
}

impl Timeline {
    async fn shutdown(&self) {
        info!("Shutting down Timeline");
        scopeguard::defer!(info!("Timeline shut down"));
        // prevent new handlers from getting created
        let handlers = self.handlers.lock().expect("mutex poisoned").take();
        if let Some(handlers) = handlers.as_ref() {
            info!("Dropping {} Arc<HandlerTimeline>s", handlers.len());
        }
        drop(handlers);
        self.gate.close().await;
    }
    fn getpage(&self, key: usize) {
        info!("get {key}");
    }
    fn rel_size(&self) {
        if self.shard_id != 0 {
            info!("rel_size not supported on shard {}", self.shard_id);
            return;
        }
        info!("rel_size");
    }
}

impl Drop for Timeline {
    fn drop(&mut self) {
        info!("Dropping Timeline");
    }
}

type TenantId = u64;
type ShardId = usize;
type TimelineId = String;

struct PageServerHandler {
    cache: HashMap<(TenantId, ShardId, TimelineId), Weak<HandlerTimeline>>,
}

impl Drop for PageServerHandler {
    fn drop(&mut self) {
        info!("Dropping PageServerHandler, unregistering its handlers from cached Timeline");
        for (_, cached) in self.cache.drain() {
            if let Some(cached) = cached.upgrade() {
                let mut lock_guard = cached.timeline.handlers.lock().expect("mutex poisoned");
                if let Some(handlers) = &mut *lock_guard {
                    let pre = handlers.len();
                    handlers.retain(|handler| !Arc::ptr_eq(handler, &cached));
                    let post = handlers.len();
                    info!("Removed {} handlers", pre - post);
                }
            }
        }
    }
}

struct HandlerTimeline {
    _gate_guard: utils::sync::gate::GateGuard,
    timeline: Arc<Timeline>,
}

impl Drop for HandlerTimeline {
    fn drop(&mut self) {
        info!("Dropping HandlerTimeline");
    }
}

impl std::ops::Deref for HandlerTimeline {
    type Target = Timeline;
    fn deref(&self) -> &Self::Target {
        &self.timeline
    }
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

#[tokio::main]
async fn main() {
    utils::logging::init(
        utils::logging::LogFormat::Plain,
        utils::logging::TracingErrorLayerEnablement::Disabled,
        utils::logging::Output::Stderr,
    )
    .unwrap();

    let timeline = Arc::new(Timeline {
        tenant_id: 23,
        shard_id: 1,
        timeline_id: "a".to_owned(),
        gate: utils::sync::gate::Gate::default(),
        handlers: Mutex::new(Some(vec![])),
    });
    // startup
    let mgr: Arc<Mutex<HashMap<(TenantId, ShardId, TimelineId), Arc<Timeline>>>> =
        Arc::new(Mutex::new(HashMap::from_iter([(
            (
                timeline.tenant_id,
                timeline.shard_id,
                timeline.timeline_id.clone(),
            ),
            timeline,
        )])));

    // page_service
    let page_service = tokio::spawn({
        let mgr = Arc::clone(&mgr);
        async move {
            let conn_loop = async move {
                loop {
                    struct Conn {}
                    impl Drop for Conn {
                        fn drop(&mut self) {
                            info!("Dropping Conn");
                        }
                    }
                    enum PagestreamRequest {
                        RelSize,
                        GetPage { key: usize },
                    }
                    impl Conn {
                        async fn read_request(&self) -> PagestreamRequest {
                            PagestreamRequest::GetPage { key: 43 }
                        }
                        async fn write_response(&self, _res: ()) {
                            tokio::time::sleep(Duration::from_millis(3000)).await;
                            info!("response written");
                        }
                    }
                    let conn = Conn {};
                    let mut handler = PageServerHandler {
                        cache: HashMap::new(),
                    };

                    // immediately enter pagestream sub-protocol, in reality there are other queries
                    let (tenant_id, timeline_id) = (23, "a".to_owned());

                    // process requests
                    loop {
                        // read request from the wire
                        let req = conn.read_request().await;

                        // shard routing based on request contents
                        let shard = match req {
                            PagestreamRequest::RelSize => {
                                // shard 0
                                let shard = 0;
                                shard
                            }
                            PagestreamRequest::GetPage { key } => {
                                // shard = key % num_shards
                                let shard = key % 2;
                                shard
                            }
                        };

                        // lookup the right shard
                        let cached_timeline: Arc<HandlerTimeline> = loop {
                            let key = (tenant_id, shard, timeline_id.clone());
                            let cached = match handler.cache.get(&key) {
                                None => {
                                    info!("handle cache miss");
                                    None
                                }
                                Some(weak) => match weak.upgrade() {
                                    None => {
                                        info!("handle cache stale");
                                        // clean up cache
                                        handler.cache.remove(&key).unwrap();
                                        None
                                    }
                                    Some(timeline) => {
                                        info!("handle cache hit");
                                        Some(timeline)
                                    }
                                },
                            };
                            match cached {
                                Some(timeline) => break timeline,
                                None => {
                                    // do the expensive mgr lookup
                                    match mgr.lock().expect("poisoned").get(&key) {
                                        Some(timeline) => {
                                            let gate_guard = match timeline.gate.enter() {
                                                Ok(guard) => guard,
                                                Err(_) => {
                                                    // gate is closed
                                                    return Err::<(), &'static str>(
                                                        "timeline: gate enter: gate is closed",
                                                    );
                                                }
                                            };
                                            let handler_timeline = Arc::new(HandlerTimeline {
                                                _gate_guard: gate_guard,
                                                timeline: Arc::clone(timeline),
                                            });
                                            {
                                                let mut lock_guard = timeline
                                                    .handlers
                                                    .lock()
                                                    .expect("mutex poisoned");
                                                match &mut *lock_guard {
                                                    Some(timeline_handlers_list) => {
                                                        for handler in timeline_handlers_list.iter() {
                                                            assert!(!Arc::ptr_eq(
                                                                handler,
                                                                &handler_timeline
                                                            ));
                                                        }
                                                        timeline_handlers_list
                                                            .push(Arc::clone(&handler_timeline));
                                                        handler.cache.insert(
                                                            key,
                                                            Arc::downgrade(&handler_timeline),
                                                        );

                                                    }
                                                    None => {
                                                        return Err("mgr: timeline shutdown started but mgr doesn't know yet");
                                                    }
                                                }
                                            }
                                            break handler_timeline;
                                        }
                                        None => return Err("mgr: timeline not found"),
                                    }
                                }
                            }
                        };

                        // execute the request against the right shard
                        let res = match req {
                            PagestreamRequest::RelSize => {
                                cached_timeline.rel_size();
                            }
                            PagestreamRequest::GetPage { key } => {
                                cached_timeline.getpage(key);
                            }
                        };

                        // drop the cached timeline before we speak protocol again, because we don't want to prevent timeline
                        // shutdown while we're speaking protocol
                        drop(cached_timeline);

                        // write response
                        conn.write_response(res).await;
                    }
                }
            };
            let res: Result<_, &str> = conn_loop.await;
            info!("conn handler exited {:?}", res);
        }.instrument(tracing::info_span!("page_service"))
    });

    // give it some uptime
    tokio::time::sleep(Duration::from_secs(7)).await;
    // ctlrc comes in, mgr shutdown
    for (_, tl) in mgr.lock().expect("poisoned").drain() {
        tl.shutdown().await;
    }
    drop(mgr);

    info!("The conn handler terminates independently, but, we have already dropped the Timeline, having shown that their lifecycles are only coupled while we're processing a request");
    page_service.await.unwrap();
    info!("conn handler exited");
}
