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
use clap::{command, Parser};
use futures_core::Stream;
use futures_util::StreamExt;
use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, StatusCode};
use metrics::{Encoder, TextEncoder};
use neon_broker::metrics::{NUM_PUBS, NUM_SUBS_ALL, NUM_SUBS_TIMELINE};
use neon_broker::{parse_proto_ttid, EitherBody, DEFAULT_LISTEN_ADDR};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::Service;
use tonic::Code;
use tonic::{Request, Response, Status};
use tracing::*;

use neon_broker::neon_broker_proto::neon_broker_server::{NeonBroker, NeonBrokerServer};
use neon_broker::neon_broker_proto::subscribe_safekeeper_info_request::SubscriptionKey as ProtoSubscriptionKey;
use neon_broker::neon_broker_proto::{SafekeeperTimelineInfo, SubscribeSafekeeperInfoRequest};
use utils::id::TenantTimelineId;
use utils::logging::{self, LogFormat};
use utils::project_git_version;

project_git_version!(GIT_VERSION);

const DEFAULT_CHAN_SIZE: usize = 256;
const DEFAULT_HTTP2_KEEPALIVE_INTERVAL: &str = "5000ms";

#[derive(Parser, Debug)]
#[command(version = GIT_VERSION, about = "Broker for neon storage nodes communication", long_about = None)]
struct Args {
    /// Endpoint to listen on.
    #[arg(short, long, default_value = DEFAULT_LISTEN_ADDR)]
    listen_addr: SocketAddr,
    /// Size of the queue to the subscriber.
    #[arg(long, default_value_t = DEFAULT_CHAN_SIZE)]
    chan_size: usize,
    /// HTTP/2 keepalive interval.
    #[arg(long, value_parser= humantime::parse_duration, default_value = DEFAULT_HTTP2_KEEPALIVE_INTERVAL)]
    http2_keepalive_interval: Duration,
    /// Format for logging, either 'plain' or 'json'.
    #[arg(long, default_value = "plain")]
    log_format: String,
}

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
    /// Add subscription to all timelines
    AddAll(Sender<SafekeeperTimelineInfo>),
    /// Add subscription to the specific timeline
    AddTimeline(TenantTimelineId, SubSender),
    /// Remove subscription to the specific timeline
    RemoveTimeline(TenantTimelineId, SubId),
    // RemoveAll is not needed as publisher will notice closed channel while
    // trying to send the next message.
}

#[derive(Default)]
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
    num_subs_to_timelines: i64,
    next_sub_id: SubId,
}

// Utility func to remove subscription from the map
fn remove_sub(
    subs_to_timelines: &mut HashMap<TenantTimelineId, Vec<SubSender>>,
    ttid: &TenantTimelineId,
    sub_id: SubId,
) {
    if let Some(subsenders) = subs_to_timelines.get_mut(ttid) {
        subsenders.retain(|ss| ss.0 != sub_id);
        if subsenders.is_empty() {
            subs_to_timelines.remove(ttid);
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
        NUM_PUBS.set(self.pub_txs.len() as i64);
        pub_id
    }

    pub fn unregister_publisher(&mut self, pub_id: PubId) {
        assert!(self.pub_txs.contains_key(&pub_id));
        self.pub_txs.remove(&pub_id);
        NUM_PUBS.set(self.pub_txs.len() as i64);
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
                NUM_SUBS_ALL.set(self.subs_to_all.len() as i64);
                SubAnnounce::AddAll(sub_tx)
            }
            SubscriptionKey::Timeline(ttid) => {
                self.subs_to_timelines
                    .entry(ttid)
                    .or_default()
                    .push(SubSender(sub_id, sub_tx.clone()));
                self.num_subs_to_timelines += 1;
                NUM_SUBS_TIMELINE.set(self.num_subs_to_timelines);
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
                NUM_SUBS_ALL.set(self.subs_to_all.len() as i64);
                None
            }
            SubscriptionKey::Timeline(ref ttid) => {
                remove_sub(&mut self.subs_to_timelines, ttid, sub_id);
                self.num_subs_to_timelines -= 1;
                NUM_SUBS_TIMELINE.set(self.num_subs_to_timelines);
                Some(SubAnnounce::RemoveTimeline(*ttid, sub_id))
            }
        };
        announce.map(|a| (self.pub_txs.values().cloned().collect(), a))
    }
}

// SharedState wrapper for post-locking operations (sending to pub_tx chans).
#[derive(Clone)]
struct Registry {
    shared_state: Arc<Mutex<SharedState>>,
    chan_size: usize,
}

const PUB_NOTIFY_CHAN_SIZE: usize = 128;

impl Registry {
    // Register new publisher in shared state.
    pub fn register_publisher(&self) -> Publisher {
        let (announce_tx, announce_rx) = mpsc::channel(PUB_NOTIFY_CHAN_SIZE);
        let mut ss = self.shared_state.lock().unwrap();
        let id = ss.register_publisher(announce_tx);
        let (subs_to_all, subs_to_timelines) = (
            ss.subs_to_all.values().cloned().collect(),
            ss.subs_to_timelines.clone(),
        );
        drop(ss);
        trace!("registered publisher {}", id);
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
        trace!("unregistered publisher {}", publisher.id);
    }

    // Register new subscriber in shared state.
    pub async fn register_subscriber(&self, sub_key: SubscriptionKey) -> Subscriber {
        let (tx, rx) = mpsc::channel(self.chan_size);
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
        trace!("registered subscriber {}", id);
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
        trace!("unregistered subscriber {}", sub.id);
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
                    warn!("dropping message, channel is full");
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
        let ttid =
            parse_proto_ttid(msg.tenant_timeline_id.as_ref().ok_or_else(|| {
                Status::new(Code::InvalidArgument, "missing tenant_timeline_id")
            })?)?;
        if let Some(subs) = self.subs_to_timelines.get(&ttid) {
            for tx in subs.iter().map(|sub_sender| &sub_sender.1) {
                if let Err(TrySendError::Full(_)) = tx.try_send(msg.clone()) {
                    warn!("dropping message, channel is full");
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

struct NeonBrokerImpl {
    registry: Registry,
}

#[tonic::async_trait]
impl NeonBroker for NeonBrokerImpl {
    async fn publish_safekeeper_info(
        &self,
        request: Request<tonic::Streaming<SafekeeperTimelineInfo>>,
    ) -> Result<Response<()>, Status> {
        let mut publisher = self.registry.register_publisher();

        let mut stream = request.into_inner();

        loop {
            select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(msg)) => publisher.send_msg(&msg)?,
                        Some(Err(e)) => return Err(e), // grpc error from the stream
                        None => break // closed stream
                    }
                }
                Some(announce) = publisher.announce_rx.next() => {
                    publisher.update_sub(announce);
                }
            }
        }

        Ok(Response::new(()))
    }

    type SubscribeSafekeeperInfoStream =
        Pin<Box<dyn Stream<Item = Result<SafekeeperTimelineInfo, Status>> + Send + 'static>>;

    async fn subscribe_safekeeper_info(
        &self,
        request: Request<SubscribeSafekeeperInfoRequest>,
    ) -> Result<Response<Self::SubscribeSafekeeperInfoStream>, Status> {
        let proto_key = request
            .into_inner()
            .subscription_key
            .ok_or_else(|| Status::new(Code::InvalidArgument, "missing subscription key"))?;
        let sub_key = SubscriptionKey::from_proto_subscription_key(proto_key)?;
        let mut subscriber = self.registry.register_subscriber(sub_key).await;

        // transform rx into stream with item = Result, as method result demands
        let output = async_stream::try_stream! {
            while let Some(info) = subscriber.sub_rx.recv().await {
                    yield info
                }
        };

        Ok(Response::new(
            Box::pin(output) as Self::SubscribeSafekeeperInfoStream
        ))
    }
}

// We serve only metrics through http1.
async fn http1_handler(
    req: hyper::Request<hyper::body::Body>,
) -> Result<hyper::Response<Body>, Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let metrics = metrics::gather();
            let encoder = TextEncoder::new();
            encoder.encode(&metrics, &mut buffer).unwrap();

            hyper::Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
                .unwrap()
        }
        _ => hyper::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    };
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    logging::init(LogFormat::from_config(&args.log_format)?)?;
    info!("version: {GIT_VERSION}");

    let registry = Registry {
        shared_state: Arc::new(Mutex::new(SharedState::default())),
        chan_size: args.chan_size,
    };
    let neon_broker_impl = NeonBrokerImpl {
        registry: registry.clone(),
    };
    let neon_broker_server = NeonBrokerServer::new(neon_broker_impl);

    info!("listening on {}", &args.listen_addr);

    // grpc is served along with http1 for metrics on a single port, hence we
    // don't use tonic's Server.
    hyper::Server::bind(&args.listen_addr)
        .http2_keep_alive_interval(Some(args.http2_keepalive_interval))
        .serve(make_service_fn(move |_| {
            let neon_broker_server_cloned = neon_broker_server.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    // Technically this second clone is not needed, but consume
                    // by async block is apparently unavoidable. BTW, error
                    // message is enigmatic, see
                    // https://github.com/rust-lang/rust/issues/68119
                    //
                    // We could get away without async block at all, but then we
                    // need to resort to futures::Either to merge the result,
                    // which doesn't caress an eye as well.
                    let mut neon_broker_server_svc = neon_broker_server_cloned.clone();
                    async move {
                        if req.headers().get("content-type").map(|x| x.as_bytes())
                            == Some(b"application/grpc")
                        {
                            let res_resp = neon_broker_server_svc.call(req).await;
                            // Grpc and http1 handlers have slightly different
                            // Response types: it is UnsyncBoxBody for the
                            // former one (not sure why) and plain hyper::Body
                            // for the latter. Both implement HttpBody though,
                            // and EitherBody is used to merge them.
                            res_resp.map(|resp| resp.map(EitherBody::Left))
                        } else {
                            let res_resp = http1_handler(req).await;
                            res_resp.map(|resp| resp.map(EitherBody::Right))
                        }
                    }
                }))
            }
        }))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use neon_broker::neon_broker_proto::TenantTimelineId as ProtoTenantTimelineId;
    use tokio::sync::mpsc::error::TryRecvError;
    use utils::id::{TenantId, TimelineId};

    fn msg(timeline_id: Vec<u8>) -> SafekeeperTimelineInfo {
        SafekeeperTimelineInfo {
            safekeeper_id: 1,
            tenant_timeline_id: Some(ProtoTenantTimelineId {
                tenant_id: vec![0x00; 16],
                timeline_id,
            }),
            last_log_term: 0,
            flush_lsn: 1,
            commit_lsn: 2,
            backup_lsn: 3,
            remote_consistent_lsn: 4,
            peer_horizon_lsn: 5,
            safekeeper_connstr: "neon-1-sk-1.local:7676".to_owned(),
            local_start_lsn: 0,
        }
    }

    fn tli_from_u64(i: u64) -> Vec<u8> {
        let mut timeline_id = vec![0xFF; 8];
        timeline_id.extend_from_slice(&i.to_be_bytes());
        timeline_id
    }

    #[tokio::test]
    async fn test_registry() {
        let registry = Registry {
            shared_state: Arc::new(Mutex::new(SharedState::default())),
            chan_size: 16,
        };

        // subscribe to timeline 2
        let ttid_2 = TenantTimelineId {
            tenant_id: TenantId::from_slice(&[0x00; 16]).unwrap(),
            timeline_id: TimelineId::from_slice(&tli_from_u64(2)).unwrap(),
        };
        let sub_key_2 = SubscriptionKey::Timeline(ttid_2);
        let mut subscriber_2 = registry.register_subscriber(sub_key_2).await;
        let mut subscriber_all = registry.register_subscriber(SubscriptionKey::All).await;

        // send two messages with different keys
        let msg_1 = msg(tli_from_u64(1));
        let msg_2 = msg(tli_from_u64(2));
        let mut publisher = registry.register_publisher();
        publisher.send_msg(&msg_1).expect("failed to send msg");
        publisher.send_msg(&msg_2).expect("failed to send msg");

        // msg with key 2 should arrive to subscriber_2
        assert!(
            matches!(subscriber_2.sub_rx.try_recv(), Ok(msg) if matches!(msg.tenant_timeline_id.as_ref(), Some(ttid) if parse_proto_ttid(ttid).unwrap() == ttid_2))
        );
        // but nothing more
        assert!(
            matches!(subscriber_2.sub_rx.try_recv(), Err(err) if matches!(err, TryRecvError::Empty))
        );

        // subscriber_all should receive both messages
        assert!(matches!(subscriber_all.sub_rx.try_recv(), Ok(..)));
        assert!(matches!(subscriber_all.sub_rx.try_recv(), Ok(..)));
        assert!(
            matches!(subscriber_all.sub_rx.try_recv(), Err(err) if matches!(err, TryRecvError::Empty))
        );
    }
}
