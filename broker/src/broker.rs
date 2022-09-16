//! Simple pub-sub based on grpc (tonic) and Tokio mpsc for storage nodes
//! messaging. The main design goal is to avoid central synchronization during
//! normal flow, resorting to it only when pub/sub change happens. Each
//! subscriber holds mpsc for messages it sits on; tx end is sent to existing
//! publishers and saved in shared state for new ones. Publishers maintain
//! locally set of subscribers they stream messages to.
//!
//! Subscriptions to 1) single timeline 2) everything are possible. We could add
//! subscription to set of timelines to save grpc streams, but testing shows
//! many individual streams is also ok.
//!
//! Message is dropped if subscriber can't consume it, not affecting other
//! subscribers.
//!
//! Only safekeeper message is supported, but it is not hard to add something
//! else with templating.
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::{select, time};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::{transport::Server, Request, Response, Status};

use neon_broker_proto::neon_broker_server::{NeonBroker, NeonBrokerServer};
use neon_broker_proto::subscribe_safekeeper_info_request::SubscriptionKey as ProtoSubscriptionKey;
use neon_broker_proto::TenantTimelineId as ProtoTenantTimelineId;
use neon_broker_proto::{Empty, SafekeeperTimelineInfo, SubscribeSafekeeperInfoRequest};
use utils::id::{TenantId, TenantTimelineId, TimelineId};

pub mod neon_broker_proto {
    // The string specified here must match the proto package name.
    // If you want to have a look at the generated code, it is at path similar to
    // target/debug/build/neon_broker-0fde81d03bedc3b2/out/neon_broker.rs
    tonic::include_proto!("neon_broker");
}

// Max size of the queue to the subscriber.
const CHAN_SIZE: usize = 256;

type PubId = u64; // id of publisher for registering in maps
type SubId = u64; // id of subscriber for registering in maps

#[derive(Copy, Clone)]
enum SubscriptionKey {
    All,
    Timeline(TenantTimelineId),
}

impl SubscriptionKey {
    // Parse protobuf subkey (protobuf doesn't have fixed size bytes, we get vectors).
    pub fn from_proto_subscription_key(key: ProtoSubscriptionKey) -> Result<Self, Status> {
        match key {
            ProtoSubscriptionKey::All(_) => Ok(SubscriptionKey::All),
            ProtoSubscriptionKey::TenantTimelineId(proto_ttid) => {
                Ok(SubscriptionKey::Timeline(parse_proto_ttid(&proto_ttid)?))
            }
        }
    }
}

// Subscriber id + tx end of the channel for messages to it.
#[derive(Clone)]
struct SubSender(SubId, Sender<SafekeeperTimelineInfo>);

impl fmt::Debug for SubSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Subscription id {}", self.0)
    }
}

// Announcements subscriber sends to publisher(s) asking it to stream to the
// provided channel, or forget about it, releasing memory.
#[derive(Clone)]
enum SubAnnounce {
    AddAll(Sender<SafekeeperTimelineInfo>), // add subscription to all timelines
    AddTimeline(TenantTimelineId, SubSender), // add subsciption to the specific timeline
    RemoveTimeline(TenantTimelineId, SubId), // remove subscription to the specific timeline
                                            // RemoveAll is not needed as publisher will notice closed channel while
                                            // trying to send the next message.
}

struct SharedState {
    // Registered publishers. They sit on the rx end of these channels and
    // receive through it tx handles of chans to subscribers.
    //
    // Note: publishers don't identify which keys they publish, so each
    // publisher will receive channels to all subs and filter them before sending.
    pub_txs: HashMap<PubId, Sender<SubAnnounce>>,
    next_pub_id: PubId,
    // Registered subscribers -- when publisher joins it walks over them,
    // collecting txs to send messages.
    subs_to_all: HashMap<SubId, Sender<SafekeeperTimelineInfo>>,
    subs_to_timelines: HashMap<TenantTimelineId, Vec<SubSender>>,
    next_sub_id: SubId,
}

// Utility func to remove subscription from the map
fn remove_sub(
    subs_to_timelines: &mut HashMap<TenantTimelineId, Vec<SubSender>>,
    ttid: &TenantTimelineId,
    sub_id: SubId,
) {
    if let Some(subsenders) = subs_to_timelines.get_mut(&ttid) {
        subsenders.retain(|ss| ss.0 != sub_id);
        if subsenders.len() == 0 {
            subs_to_timelines.remove(&ttid);
        }
    }
    // Note that subscription might be not here if subscriber task was aborted
    // earlier than it managed to notify publisher about itself.
}

impl SharedState {
    // Register new publisher.
    pub fn register_publisher(&mut self, announce_tx: Sender<SubAnnounce>) -> PubId {
        let pub_id = self.next_pub_id;
        self.next_pub_id += 1;
        assert!(!self.pub_txs.contains_key(&pub_id));
        self.pub_txs.insert(pub_id, announce_tx);
        pub_id
    }

    pub fn unregister_publisher(&mut self, pub_id: PubId) {
        assert!(self.pub_txs.contains_key(&pub_id));
        self.pub_txs.remove(&pub_id);
    }

    // Register new subscriber.
    // Returns list of channels through which existing publishers must be notified
    // about new subscriber; we can't do it here due to risk of deadlock.
    pub fn register_subscriber(
        &mut self,
        sub_key: SubscriptionKey,
        sub_tx: Sender<SafekeeperTimelineInfo>,
    ) -> (SubId, Vec<Sender<SubAnnounce>>, SubAnnounce) {
        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        let announce = match sub_key {
            SubscriptionKey::All => {
                assert!(!self.subs_to_all.contains_key(&sub_id));
                self.subs_to_all.insert(sub_id, sub_tx.clone());
                SubAnnounce::AddAll(sub_tx)
            }
            SubscriptionKey::Timeline(ttid) => {
                match self.subs_to_timelines.entry(ttid) {
                    Entry::Occupied(mut o) => {
                        let subsenders = o.get_mut();
                        subsenders.push(SubSender(sub_id, sub_tx.clone()));
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![SubSender(sub_id, sub_tx.clone())]);
                    }
                }
                SubAnnounce::AddTimeline(ttid, SubSender(sub_id, sub_tx))
            }
        };
        // Collect existing publishers to notify them after lock is released;
        // TODO: the probability of channels being full here is tiny (publisher
        // always blocks listening chan), we can try sending first and resort to
        // cloning if needed.
        //
        // Deadlock is possible only if publisher tries to access shared state
        // during its lifetime, i.e. we add maintenance of set of published
        // tlis. Otherwise we can just await here (but lock must be replaced
        // with Tokio one).
        //
        // We could also just error out if some chan is full, but that needs
        // cleanup of incompleted job, and notifying publishers when unregistering
        // is mandatory anyway.
        (sub_id, self.pub_txs.values().cloned().collect(), announce)
    }

    // Unregister the subscriber. Similar to register_subscriber, returns list
    // of channels through which publishers must be notified about the removal.
    pub fn unregister_subscriber(
        &mut self,
        sub_id: SubId,
        sub_key: SubscriptionKey,
    ) -> Option<(Vec<Sender<SubAnnounce>>, SubAnnounce)> {
        // We need to notify existing publishers only about per timeline
        // subscriptions, 'all' kind is detected on its own through closed
        // channels.
        let announce = match sub_key {
            SubscriptionKey::All => {
                assert!(self.subs_to_all.contains_key(&sub_id));
                self.subs_to_all.remove(&sub_id);
                None
            }
            SubscriptionKey::Timeline(ref ttid) => {
                remove_sub(&mut self.subs_to_timelines, ttid, sub_id);
                Some(SubAnnounce::RemoveTimeline(*ttid, sub_id))
            }
        };
        announce.map(|a| (self.pub_txs.values().cloned().collect(), a))
    }

    pub fn report(&mut self) {
        println!(
            "registered {} publishers, {} subs to all, {} subs to timelines",
            self.pub_txs.len(),
            self.subs_to_all.len(),
            self.subs_to_timelines.len(),
        );
    }
}

// SharedState wrapper for post-locking operations (sending to pub_tx chans).
#[derive(Clone)]
struct Registry {
    shared_state: Arc<Mutex<SharedState>>,
}

impl Registry {
    // Register new publisher in shared state.
    pub fn register_publisher(&self) -> Publisher {
        let (announce_tx, announce_rx) = mpsc::channel(128);
        let mut ss = self.shared_state.lock().unwrap();
        let id = ss.register_publisher(announce_tx);
        let (subs_to_all, subs_to_timelines) = (
            ss.subs_to_all.values().cloned().collect(),
            ss.subs_to_timelines.clone(),
        );
        drop(ss);
        // println!("registered publisher {}", id);
        Publisher {
            id,
            announce_rx: announce_rx.into(),
            subs_to_all,
            subs_to_timelines,
            registry: self.clone(),
        }
    }

    pub fn unregister_publisher(&self, publisher: &Publisher) {
        self.shared_state
            .lock()
            .unwrap()
            .unregister_publisher(publisher.id);
        // println!("unregistered publisher {}", publisher.id);
    }

    // Register new subscriber in shared state.
    pub async fn register_subscriber(&self, sub_key: SubscriptionKey) -> Subscriber {
        let (tx, rx) = mpsc::channel(CHAN_SIZE);
        let id;
        let mut pub_txs;
        let announce;
        {
            let mut ss = self.shared_state.lock().unwrap();
            (id, pub_txs, announce) = ss.register_subscriber(sub_key, tx);
        }
        // Note: it is important to create Subscriber before .await. If client
        // disconnects during await, which would terminate the Future we still
        // need to run Subscriber's drop() which will unregister it from the
        // shared state.
        let subscriber = Subscriber {
            id,
            key: sub_key,
            sub_rx: rx,
            registry: self.clone(),
        };
        // Notify existing publishers about new subscriber.
        for pub_tx in pub_txs.iter_mut() {
            // Closed channel is fine; it means publisher has gone.
            pub_tx.send(announce.clone()).await.ok();
        }
        // println!("registered subscriber {}", id);
        subscriber
    }

    // Unregister the subscriber
    pub fn unregister_subscriber(&self, sub: &Subscriber) {
        let mut ss = self.shared_state.lock().unwrap();
        let announce_pack = ss.unregister_subscriber(sub.id, sub.key);
        drop(ss);
        // Notify publishers about the removal. Apart from wanting to do it
        // outside lock, here we also spin a task as Drop impl can't be async.
        if let Some((mut pub_txs, announce)) = announce_pack {
            tokio::spawn(async move {
                for pub_tx in pub_txs.iter_mut() {
                    // Closed channel is fine; it means publisher has gone.
                    pub_tx.send(announce.clone()).await.ok();
                }
            });
        }
        // println!("unregistered subscriber {}", sub.id);
    }

    pub async fn report(&self) {
        let mut interval = time::interval(Duration::from_millis(1000));
        loop {
            interval.tick().await;
            self.shared_state.lock().unwrap().report();
        }
    }
}

// Private subscriber state.
struct Subscriber {
    id: SubId,
    key: SubscriptionKey,
    // Subscriber receives messages from publishers here.
    sub_rx: Receiver<SafekeeperTimelineInfo>,
    // to unregister itself from shared state in Drop
    registry: Registry,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.registry.unregister_subscriber(self);
    }
}

// Private publisher state
struct Publisher {
    id: PubId,
    // new subscribers request to send (or stop sending) msgs them here.
    // It could be just Receiver, but weirdly it doesn't implement futures_core Stream directly.
    announce_rx: ReceiverStream<SubAnnounce>,
    subs_to_all: Vec<Sender<SafekeeperTimelineInfo>>,
    subs_to_timelines: HashMap<TenantTimelineId, Vec<SubSender>>,
    // to unregister itself from shared state in Drop
    registry: Registry,
}

impl Publisher {
    // Send msg to relevant subscribers.
    pub fn send_msg(&mut self, msg: &SafekeeperTimelineInfo) -> Result<(), Status> {
        // send message to subscribers for everything
        let mut cleanup_subs_to_all = false;
        for sub in self.subs_to_all.iter() {
            match sub.try_send(msg.clone()) {
                Err(TrySendError::Full(_)) => {
                    // println!("dropping message, channel is full");
                }
                Err(TrySendError::Closed(_)) => {
                    cleanup_subs_to_all = true;
                }
                _ => (),
            }
        }
        // some channels got closed (subscriber gone), remove them
        if cleanup_subs_to_all {
            self.subs_to_all.retain(|tx| !tx.is_closed());
        }

        // send message to per timeline subscribers
        let ttid = parse_proto_ttid(msg.tenant_timeline_id.as_ref().ok_or(Status::new(
            Code::InvalidArgument,
            "missing tenant_timeline_id",
        ))?)?;
        if let Some(subs) = self.subs_to_timelines.get(&ttid) {
            for tx in subs.iter().map(|sub_sender| &sub_sender.1) {
                if let Err(TrySendError::Full(_)) = tx.try_send(msg.clone()) {
                    // println!("dropping message, channel is full");
                }
                // closed channel is ignored here; we will be notified and remove it soon
            }
        }
        Ok(())
    }

    // Add/remove subscriber according to sub_announce.
    pub fn update_sub(&mut self, sub_announce: SubAnnounce) {
        match sub_announce {
            SubAnnounce::AddAll(tx) => self.subs_to_all.push(tx),
            SubAnnounce::AddTimeline(ttid, sub_sender) => {
                match self.subs_to_timelines.entry(ttid) {
                    Entry::Occupied(mut o) => {
                        let subsenders = o.get_mut();
                        subsenders.push(sub_sender);
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![sub_sender]);
                    }
                }
            }
            SubAnnounce::RemoveTimeline(ref ttid, sub_id) => {
                remove_sub(&mut self.subs_to_timelines, ttid, sub_id);
            }
        }
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.registry.unregister_publisher(self);
    }
}

struct NeonBrokerService {
    registry: Registry,
}

#[tonic::async_trait]
impl NeonBroker for NeonBrokerService {
    async fn publish_safekeeper_info(
        &self,
        request: Request<tonic::Streaming<SafekeeperTimelineInfo>>,
    ) -> Result<Response<Empty>, Status> {
        let mut publisher = self.registry.register_publisher();

        let mut stream = request.into_inner();

        loop {
            select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(msg)) => {publisher.send_msg(&msg)?;},
                        Some(Err(e)) => {return Err(e);}, // grpc error from the stream
                        None => {break;} // closed stream
                    }
                }
                Some(announce) = publisher.announce_rx.next() => {
                    publisher.update_sub(announce);
                }
            }
        }

        Ok(Response::new(Empty {}))
    }

    type SubscribeSafekeeperInfoStream =
        Pin<Box<dyn Stream<Item = Result<SafekeeperTimelineInfo, Status>> + Send + 'static>>;

    async fn subscribe_safekeeper_info(
        &self,
        request: Request<SubscribeSafekeeperInfoRequest>,
    ) -> Result<Response<Self::SubscribeSafekeeperInfoStream>, Status> {
        let proto_key = request.into_inner().subscription_key.ok_or(Status::new(
            Code::InvalidArgument,
            "missing subscription key",
        ))?;
        let sub_key = SubscriptionKey::from_proto_subscription_key(proto_key)?;
        let mut subscriber = self.registry.register_subscriber(sub_key).await;

        // transform rx into stream with item = Result, as method result demands
        let output = async_stream::try_stream! {
            while let Some(info) = subscriber.sub_rx.recv().await {
                    yield info
                }

            // internal generator
            // let _ = subscriber.sub_rx.try_recv().ok();
            // let mut counter = 0;
            // loop {
            // let info = SafekeeperTimelineInfo {
            //     safekeeper_id: 1,
            //     tenant_timeline_id: Some(ProtoTenantTimelineId {
            //         tenant_id: vec![0xFF; 16],
            //         timeline_id: vec![0xFF; 16],
            //         // timeline_id: tli_from_u64(counter),
            //     }),
            //     last_log_term: 0,
            //     flush_lsn: counter,
            //     commit_lsn: 2,
            //     backup_lsn: 3,
            //     remote_consistent_lsn: 4,
            //     peer_horizon_lsn: 5,
            //     safekeeper_connstr: "zenith-1-sk-1.local:7676".to_owned(),
            // };
            // counter += 1;
            // yield info;
            // }
        };

        Ok(Response::new(
            Box::pin(output) as Self::SubscribeSafekeeperInfoStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // console_subscriber::init();

    let addr = "[::1]:50051".parse()?;
    let registry = Registry {
        shared_state: Arc::new(Mutex::new(SharedState {
            pub_txs: HashMap::new(),
            next_pub_id: 0,
            subs_to_all: HashMap::new(),
            subs_to_timelines: HashMap::new(),
            next_sub_id: 0,
        })),
    };
    let neon_broker_service = NeonBrokerService {
        registry: registry.clone(),
    };

    tokio::spawn(async move { registry.report().await });

    Server::builder()
        .http2_keepalive_interval(Some(Duration::from_millis(5000)))
        .add_service(NeonBrokerServer::new(neon_broker_service))
        .serve(addr)
        .await?;

    Ok(())
}

// parse variable length bytes from protobuf
fn parse_proto_ttid(proto_ttid: &ProtoTenantTimelineId) -> Result<TenantTimelineId, Status> {
    let tenant_id = TenantId::from_vec(&proto_ttid.tenant_id)
        .map_err(|e| Status::new(Code::InvalidArgument, format!("malformed tenant_id: {}", e)))?;
    let timeline_id = TimelineId::from_vec(&proto_ttid.timeline_id).map_err(|e| {
        Status::new(
            Code::InvalidArgument,
            format!("malformed timeline_id: {}", e),
        )
    })?;
    Ok(TenantTimelineId {
        tenant_id,
        timeline_id,
    })
}
