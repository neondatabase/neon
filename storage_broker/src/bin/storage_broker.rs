//! Simple pub-sub based on grpc (tonic) and Tokio broadcast channel for storage
//! nodes messaging.
//!
//! Subscriptions to 1) single timeline 2) all timelines are possible. We could
//! add subscription to the set of timelines to save grpc streams, but testing
//! shows many individual streams is also ok.
//!
//! Message is dropped if subscriber can't consume it, not affecting other
//! subscribers.
//!
//! Only safekeeper message is supported, but it is not hard to add something
//! else with generics.
use clap::{command, Parser};
use futures_core::Stream;
use futures_util::StreamExt;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, StatusCode};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::time;
use tonic::codegen::Service;
use tonic::transport::server::Connected;
use tonic::Code;
use tonic::{Request, Response, Status};
use tracing::*;
use utils::signals::ShutdownSignals;

use metrics::{Encoder, TextEncoder};
use storage_broker::metrics::{NUM_PUBS, NUM_SUBS_ALL, NUM_SUBS_TIMELINE};
use storage_broker::proto::broker_service_server::{BrokerService, BrokerServiceServer};
use storage_broker::proto::subscribe_safekeeper_info_request::SubscriptionKey as ProtoSubscriptionKey;
use storage_broker::proto::{SafekeeperTimelineInfo, SubscribeSafekeeperInfoRequest};
use storage_broker::{
    parse_proto_ttid, EitherBody, DEFAULT_KEEPALIVE_INTERVAL, DEFAULT_LISTEN_ADDR,
};
use utils::id::TenantTimelineId;
use utils::logging::{self, LogFormat};
use utils::project_git_version;
use utils::sentry_init::init_sentry;

project_git_version!(GIT_VERSION);

const DEFAULT_CHAN_SIZE: usize = 32;
const DEFAULT_ALL_KEYS_CHAN_SIZE: usize = 16384;

#[derive(Parser, Debug)]
#[command(version = GIT_VERSION, about = "Broker for neon storage nodes communication", long_about = None)]
struct Args {
    /// Endpoint to listen on.
    #[arg(short, long, default_value = DEFAULT_LISTEN_ADDR)]
    listen_addr: SocketAddr,
    /// Size of the queue to the per timeline subscriber.
    #[arg(long, default_value_t = DEFAULT_CHAN_SIZE)]
    timeline_chan_size: usize,
    /// Size of the queue to the all keys subscriber.
    #[arg(long, default_value_t = DEFAULT_ALL_KEYS_CHAN_SIZE)]
    all_keys_chan_size: usize,
    /// HTTP/2 keepalive interval.
    #[arg(long, value_parser= humantime::parse_duration, default_value = DEFAULT_KEEPALIVE_INTERVAL)]
    http2_keepalive_interval: Duration,
    /// Format for logging, either 'plain' or 'json'.
    #[arg(long, default_value = "plain")]
    log_format: String,
}

type PubId = u64; // id of publisher for registering in maps
type SubId = u64; // id of subscriber for registering in maps

#[derive(Copy, Clone, Debug)]
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

// Channel to timeline subscribers.
struct ChanToTimelineSub {
    chan: broadcast::Sender<SafekeeperTimelineInfo>,
    // Tracked separately to know when delete the shmem entry. receiver_count()
    // is unhandy for that as unregistering and dropping the receiver side
    // happens at different moments.
    num_subscribers: u64,
}

struct SharedState {
    next_pub_id: PubId,
    num_pubs: i64,
    next_sub_id: SubId,
    num_subs_to_timelines: i64,
    chans_to_timeline_subs: HashMap<TenantTimelineId, ChanToTimelineSub>,
    num_subs_to_all: i64,
    chan_to_all_subs: broadcast::Sender<SafekeeperTimelineInfo>,
}

impl SharedState {
    pub fn new(all_keys_chan_size: usize) -> Self {
        SharedState {
            next_pub_id: 0,
            num_pubs: 0,
            next_sub_id: 0,
            num_subs_to_timelines: 0,
            chans_to_timeline_subs: HashMap::new(),
            num_subs_to_all: 0,
            chan_to_all_subs: broadcast::channel(all_keys_chan_size).0,
        }
    }

    // Register new publisher.
    pub fn register_publisher(&mut self) -> PubId {
        let pub_id = self.next_pub_id;
        self.next_pub_id += 1;
        self.num_pubs += 1;
        NUM_PUBS.set(self.num_pubs);
        pub_id
    }

    // Unregister publisher.
    pub fn unregister_publisher(&mut self) {
        self.num_pubs -= 1;
        NUM_PUBS.set(self.num_pubs);
    }

    // Register new subscriber.
    pub fn register_subscriber(
        &mut self,
        sub_key: SubscriptionKey,
        timeline_chan_size: usize,
    ) -> (SubId, broadcast::Receiver<SafekeeperTimelineInfo>) {
        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        let sub_rx = match sub_key {
            SubscriptionKey::All => {
                self.num_subs_to_all += 1;
                NUM_SUBS_ALL.set(self.num_subs_to_all);
                self.chan_to_all_subs.subscribe()
            }
            SubscriptionKey::Timeline(ttid) => {
                self.num_subs_to_timelines += 1;
                NUM_SUBS_TIMELINE.set(self.num_subs_to_timelines);
                // Create new broadcast channel for this key, or subscriber to
                // the existing one.
                let chan_to_timeline_sub =
                    self.chans_to_timeline_subs
                        .entry(ttid)
                        .or_insert(ChanToTimelineSub {
                            chan: broadcast::channel(timeline_chan_size).0,
                            num_subscribers: 0,
                        });
                chan_to_timeline_sub.num_subscribers += 1;
                chan_to_timeline_sub.chan.subscribe()
            }
        };
        (sub_id, sub_rx)
    }

    // Unregister the subscriber.
    pub fn unregister_subscriber(&mut self, sub_key: SubscriptionKey) {
        match sub_key {
            SubscriptionKey::All => {
                self.num_subs_to_all -= 1;
                NUM_SUBS_ALL.set(self.num_subs_to_all);
            }
            SubscriptionKey::Timeline(ttid) => {
                self.num_subs_to_timelines -= 1;
                NUM_SUBS_TIMELINE.set(self.num_subs_to_timelines);

                // Remove from the map, destroying the channel, if we are the
                // last subscriber to this timeline.

                // Missing entry is a bug; we must have registered.
                let chan_to_timeline_sub = self
                    .chans_to_timeline_subs
                    .get_mut(&ttid)
                    .expect("failed to find sub entry in shmem during unregister");
                chan_to_timeline_sub.num_subscribers -= 1;
                if chan_to_timeline_sub.num_subscribers == 0 {
                    self.chans_to_timeline_subs.remove(&ttid);
                }
            }
        }
    }
}

// SharedState wrapper.
#[derive(Clone)]
struct Registry {
    shared_state: Arc<RwLock<SharedState>>,
    timeline_chan_size: usize,
}

impl Registry {
    // Register new publisher in shared state.
    pub fn register_publisher(&self, remote_addr: SocketAddr) -> Publisher {
        let pub_id = self.shared_state.write().register_publisher();
        info!("publication started id={} addr={:?}", pub_id, remote_addr);
        Publisher {
            id: pub_id,
            registry: self.clone(),
            remote_addr,
        }
    }

    pub fn unregister_publisher(&self, publisher: &Publisher) {
        self.shared_state.write().unregister_publisher();
        info!(
            "publication ended id={} addr={:?}",
            publisher.id, publisher.remote_addr
        );
    }

    // Register new subscriber in shared state.
    pub fn register_subscriber(
        &self,
        sub_key: SubscriptionKey,
        remote_addr: SocketAddr,
    ) -> Subscriber {
        let (sub_id, sub_rx) = self
            .shared_state
            .write()
            .register_subscriber(sub_key, self.timeline_chan_size);
        info!(
            "subscription started id={}, key={:?}, addr={:?}",
            sub_id, sub_key, remote_addr
        );
        Subscriber {
            id: sub_id,
            key: sub_key,
            sub_rx,
            registry: self.clone(),
            remote_addr,
        }
    }

    // Unregister the subscriber
    pub fn unregister_subscriber(&self, subscriber: &Subscriber) {
        self.shared_state
            .write()
            .unregister_subscriber(subscriber.key);
        info!(
            "subscription ended id={}, key={:?}, addr={:?}",
            subscriber.id, subscriber.key, subscriber.remote_addr
        );
    }
}

// Private subscriber state.
struct Subscriber {
    id: SubId,
    key: SubscriptionKey,
    // Subscriber receives messages from publishers here.
    sub_rx: broadcast::Receiver<SafekeeperTimelineInfo>,
    // to unregister itself from shared state in Drop
    registry: Registry,
    // for logging
    remote_addr: SocketAddr,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.registry.unregister_subscriber(self);
    }
}

// Private publisher state
struct Publisher {
    id: PubId,
    registry: Registry,
    // for logging
    remote_addr: SocketAddr,
}

impl Publisher {
    // Send msg to relevant subscribers.
    pub fn send_msg(&mut self, msg: &SafekeeperTimelineInfo) -> Result<(), Status> {
        // send message to subscribers for everything
        let shared_state = self.registry.shared_state.read();
        // Err means there is no subscribers, it is fine.
        shared_state.chan_to_all_subs.send(msg.clone()).ok();

        // send message to per timeline subscribers
        let ttid =
            parse_proto_ttid(msg.tenant_timeline_id.as_ref().ok_or_else(|| {
                Status::new(Code::InvalidArgument, "missing tenant_timeline_id")
            })?)?;
        if let Some(subs) = shared_state.chans_to_timeline_subs.get(&ttid) {
            // Err can't happen here, as tx is destroyed only after removing
            // from the map the last subscriber along with tx.
            subs.chan
                .send(msg.clone())
                .expect("rx is still in the map with zero subscribers");
        }
        Ok(())
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.registry.unregister_publisher(self);
    }
}

struct Broker {
    registry: Registry,
}

#[tonic::async_trait]
impl BrokerService for Broker {
    async fn publish_safekeeper_info(
        &self,
        request: Request<tonic::Streaming<SafekeeperTimelineInfo>>,
    ) -> Result<Response<()>, Status> {
        let remote_addr = request
            .remote_addr()
            .expect("TCPConnectInfo inserted by handler");
        let mut publisher = self.registry.register_publisher(remote_addr);

        let mut stream = request.into_inner();

        loop {
            match stream.next().await {
                Some(Ok(msg)) => publisher.send_msg(&msg)?,
                Some(Err(e)) => return Err(e), // grpc error from the stream
                None => break,                 // closed stream
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
        let remote_addr = request
            .remote_addr()
            .expect("TCPConnectInfo inserted by handler");
        let proto_key = request
            .into_inner()
            .subscription_key
            .ok_or_else(|| Status::new(Code::InvalidArgument, "missing subscription key"))?;
        let sub_key = SubscriptionKey::from_proto_subscription_key(proto_key)?;
        let mut subscriber = self.registry.register_subscriber(sub_key, remote_addr);

        // transform rx into stream with item = Result, as method result demands
        let output = async_stream::try_stream! {
            let mut warn_interval = time::interval(Duration::from_millis(1000));
            let mut missed_msgs: u64 = 0;
            loop {
                match subscriber.sub_rx.recv().await {
                    Ok(info) => yield info,
                    Err(RecvError::Lagged(skipped_msg)) => {
                        missed_msgs += skipped_msg;
                        if (futures::poll!(Box::pin(warn_interval.tick()))).is_ready() {
                            warn!("subscription id={}, key={:?} addr={:?} dropped {} messages, channel is full",
                                subscriber.id, subscriber.key, subscriber.remote_addr, missed_msgs);
                            missed_msgs = 0;
                        }
                    }
                    Err(RecvError::Closed) => {
                        // can't happen, we never drop the channel while there is a subscriber
                        Err(Status::new(Code::Internal, "channel unexpectantly closed"))?;
                    }
                }
            }
        };

        Ok(Response::new(
            Box::pin(output) as Self::SubscribeSafekeeperInfoStream
        ))
    }
}

// We serve only metrics and healthcheck through http1.
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
        (&Method::GET, "/status") => hyper::Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
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

    // important to keep the order of:
    // 1. init logging
    // 2. tracing panic hook
    // 3. sentry
    logging::init(
        LogFormat::from_config(&args.log_format)?,
        logging::TracingErrorLayerEnablement::Disabled,
    )?;
    logging::replace_panic_hook_with_tracing_panic_hook().forget();
    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);
    info!("version: {GIT_VERSION}");
    ::metrics::set_build_info_metric(GIT_VERSION);

    // On any shutdown signal, log receival and exit.
    std::thread::spawn(move || {
        ShutdownSignals::handle(|signal| {
            info!("received {}, terminating", signal.name());
            std::process::exit(0);
        })
    });

    let registry = Registry {
        shared_state: Arc::new(RwLock::new(SharedState::new(args.all_keys_chan_size))),
        timeline_chan_size: args.timeline_chan_size,
    };
    let storage_broker_impl = Broker {
        registry: registry.clone(),
    };
    let storage_broker_server = BrokerServiceServer::new(storage_broker_impl);

    info!("listening on {}", &args.listen_addr);

    // grpc is served along with http1 for metrics on a single port, hence we
    // don't use tonic's Server.
    hyper::Server::bind(&args.listen_addr)
        .http2_keep_alive_interval(Some(args.http2_keepalive_interval))
        .serve(make_service_fn(move |conn: &AddrStream| {
            let storage_broker_server_cloned = storage_broker_server.clone();
            let connect_info = conn.connect_info();
            async move {
                Ok::<_, Infallible>(service_fn(move |mut req| {
                    // That's what tonic's MakeSvc.call does to pass conninfo to
                    // the request handler (and where its request.remote_addr()
                    // expects it to find).
                    req.extensions_mut().insert(connect_info.clone());

                    // Technically this second clone is not needed, but consume
                    // by async block is apparently unavoidable. BTW, error
                    // message is enigmatic, see
                    // https://github.com/rust-lang/rust/issues/68119
                    //
                    // We could get away without async block at all, but then we
                    // need to resort to futures::Either to merge the result,
                    // which doesn't caress an eye as well.
                    let mut storage_broker_server_svc = storage_broker_server_cloned.clone();
                    async move {
                        if req.headers().get("content-type").map(|x| x.as_bytes())
                            == Some(b"application/grpc")
                        {
                            let res_resp = storage_broker_server_svc.call(req).await;
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
    use storage_broker::proto::TenantTimelineId as ProtoTenantTimelineId;
    use tokio::sync::broadcast::error::TryRecvError;
    use utils::id::{TenantId, TimelineId};

    fn msg(timeline_id: Vec<u8>) -> SafekeeperTimelineInfo {
        SafekeeperTimelineInfo {
            safekeeper_id: 1,
            tenant_timeline_id: Some(ProtoTenantTimelineId {
                tenant_id: vec![0x00; 16],
                timeline_id,
            }),
            term: 0,
            last_log_term: 0,
            flush_lsn: 1,
            commit_lsn: 2,
            backup_lsn: 3,
            remote_consistent_lsn: 4,
            peer_horizon_lsn: 5,
            safekeeper_connstr: "neon-1-sk-1.local:7676".to_owned(),
            http_connstr: "neon-1-sk-1.local:7677".to_owned(),
            local_start_lsn: 0,
            availability_zone: None,
        }
    }

    fn tli_from_u64(i: u64) -> Vec<u8> {
        let mut timeline_id = vec![0xFF; 8];
        timeline_id.extend_from_slice(&i.to_be_bytes());
        timeline_id
    }

    fn mock_addr() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    #[tokio::test]
    async fn test_registry() {
        let registry = Registry {
            shared_state: Arc::new(RwLock::new(SharedState::new(16))),
            timeline_chan_size: 16,
        };

        // subscribe to timeline 2
        let ttid_2 = TenantTimelineId {
            tenant_id: TenantId::from_slice(&[0x00; 16]).unwrap(),
            timeline_id: TimelineId::from_slice(&tli_from_u64(2)).unwrap(),
        };
        let sub_key_2 = SubscriptionKey::Timeline(ttid_2);
        let mut subscriber_2 = registry.register_subscriber(sub_key_2, mock_addr());
        let mut subscriber_all = registry.register_subscriber(SubscriptionKey::All, mock_addr());

        // send two messages with different keys
        let msg_1 = msg(tli_from_u64(1));
        let msg_2 = msg(tli_from_u64(2));
        let mut publisher = registry.register_publisher(mock_addr());
        publisher.send_msg(&msg_1).expect("failed to send msg");
        publisher.send_msg(&msg_2).expect("failed to send msg");

        // msg with key 2 should arrive to subscriber_2
        assert_eq!(subscriber_2.sub_rx.try_recv().unwrap(), msg_2);

        // but nothing more
        assert_eq!(
            subscriber_2.sub_rx.try_recv().unwrap_err(),
            TryRecvError::Empty
        );

        // subscriber_all should receive both messages
        assert_eq!(subscriber_all.sub_rx.try_recv().unwrap(), msg_1);
        assert_eq!(subscriber_all.sub_rx.try_recv().unwrap(), msg_2);
        assert_eq!(
            subscriber_all.sub_rx.try_recv().unwrap_err(),
            TryRecvError::Empty
        );
    }
}
