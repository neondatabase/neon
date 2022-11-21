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
//! else with generics.
use clap::{command, Parser};
use futures_core::Stream;
use futures_util::StreamExt;
use hyper::header::CONTENT_TYPE;
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
use tonic::codegen::Service;
use tonic::Code;
use tonic::{Request, Response, Status};
use tracing::*;

use metrics::{Encoder, TextEncoder};
use neon_broker::metrics::{NUM_PUBS, NUM_SUBS_ALL, NUM_SUBS_TIMELINE};
use neon_broker::neon_broker_proto::neon_broker_server::{NeonBroker, NeonBrokerServer};
use neon_broker::neon_broker_proto::subscribe_safekeeper_info_request::SubscriptionKey as ProtoSubscriptionKey;
use neon_broker::neon_broker_proto::{SafekeeperTimelineInfo, SubscribeSafekeeperInfoRequest};
use neon_broker::{parse_proto_ttid, EitherBody, DEFAULT_LISTEN_ADDR};
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
    pub fn new(chan_size: usize) -> Self {
        SharedState {
            next_pub_id: 0,
            num_pubs: 0,
            next_sub_id: 0,
            num_subs_to_timelines: 0,
            chans_to_timeline_subs: HashMap::new(),
            num_subs_to_all: 0,
            chan_to_all_subs: broadcast::channel(chan_size).0,
        }
    }

    // Register new publisher.
    pub fn register_publisher(&mut self, registry: Registry) -> Publisher {
        let pub_id = self.next_pub_id;
        self.next_pub_id += 1;
        self.num_pubs += 1;
        NUM_PUBS.set(self.num_pubs);
        Publisher {
            id: pub_id,
            registry,
        }
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
        registry: Registry,
        chan_size: usize,
    ) -> Subscriber {
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
                            chan: broadcast::channel(chan_size).0,
                            num_subscribers: 0,
                        });
                chan_to_timeline_sub.num_subscribers += 1;
                chan_to_timeline_sub.chan.subscribe()
            }
        };
        Subscriber {
            id: sub_id,
            key: sub_key,
            sub_rx,
            registry,
        }
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
                let chan_to_timeline_sub = self.chans_to_timeline_subs.get_mut(&ttid).unwrap();
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
    chan_size: usize,
}

impl Registry {
    // Register new publisher in shared state.
    pub fn register_publisher(&self) -> Publisher {
        let publisher = self.shared_state.write().register_publisher(self.clone());
        trace!("registered publisher {}", publisher.id);
        publisher
    }

    pub fn unregister_publisher(&self, publisher: &Publisher) {
        self.shared_state.write().unregister_publisher();
        trace!("unregistered publisher {}", publisher.id);
    }

    // Register new subscriber in shared state.
    pub fn register_subscriber(&self, sub_key: SubscriptionKey) -> Subscriber {
        let subscriber =
            self.shared_state
                .write()
                .register_subscriber(sub_key, self.clone(), self.chan_size);
        trace!("registered subscriber {}", subscriber.id);
        subscriber
    }

    // Unregister the subscriber
    pub fn unregister_subscriber(&self, subscriber: &Subscriber) {
        self.shared_state
            .write()
            .unregister_subscriber(subscriber.key);
        trace!("unregistered subscriber {}", subscriber.id);
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
            // Err can't happen here, as rx is destroyed after removing from the
            // map the last subscriber.
            subs.chan.send(msg.clone()).unwrap();
        }
        Ok(())
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
        let proto_key = request
            .into_inner()
            .subscription_key
            .ok_or_else(|| Status::new(Code::InvalidArgument, "missing subscription key"))?;
        let sub_key = SubscriptionKey::from_proto_subscription_key(proto_key)?;
        let mut subscriber = self.registry.register_subscriber(sub_key);

        // transform rx into stream with item = Result, as method result demands
        let output = async_stream::try_stream! {
            loop {
                match subscriber.sub_rx.recv().await {
                    Ok(info) => yield info,
                    Err(RecvError::Lagged(_skipped_msg)) => {
                        // warn!("dropped {} messages, channel is full", skipped_msg);
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
        shared_state: Arc::new(RwLock::new(SharedState::new(args.chan_size))),
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
    use tokio::sync::broadcast::error::TryRecvError;
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
            shared_state: Arc::new(RwLock::new(SharedState::new(16))),
            chan_size: 16,
        };

        // subscribe to timeline 2
        let ttid_2 = TenantTimelineId {
            tenant_id: TenantId::from_slice(&[0x00; 16]).unwrap(),
            timeline_id: TimelineId::from_slice(&tli_from_u64(2)).unwrap(),
        };
        let sub_key_2 = SubscriptionKey::Timeline(ttid_2);
        let mut subscriber_2 = registry.register_subscriber(sub_key_2);
        let mut subscriber_all = registry.register_subscriber(SubscriptionKey::All);

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
