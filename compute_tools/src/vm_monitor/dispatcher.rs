//! Managing the websocket connection and other signals in the monitor.
//!
//! Contains types that manage the interaction (not data interchange, see `protocol`)
//! between informant and monitor, allowing us to to process and send messages in a
//! straightforward way. The dispatcher also manages that signals that come from
//! the cgroup (requesting upscale), and the signals that go to the cgroup
//! (notifying it of upscale).

use anyhow::{bail, Context};
use async_std::channel::{Receiver, Sender};
use axum::extract::ws::{Message, WebSocket};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tracing::info;

use crate::vm_monitor::cgroup::Sequenced;
use crate::vm_monitor::protocol::{
    OutboundMsg, ProtocolRange, ProtocolResponse, ProtocolVersion, Resources, PROTOCOL_MAX_VERSION,
    PROTOCOL_MIN_VERSION,
};

/// The central handler for all communications in the monitor.
///
/// The dispatcher has two purposes:
/// 1. Manage the connection to the informant, sending and receiving messages.
/// 2. Communicate with the cgroup manager, notifying it when upscale is received,
///    and sending a message to the informant when the cgroup manager requests
///    upscale.
#[derive(Debug)]
pub struct Dispatcher {
    /// We read informant messages of of `source`
    pub(crate) source: SplitStream<WebSocket>,

    /// We send messages to the informant through `sink`
    sink: SplitSink<WebSocket, Message>,

    /// Used to notify the cgroup when we are upscaled. The manager acknowledges
    /// on the `oneshot::Sender` once it is done handling the upscale.
    pub(crate) notify_upscale_events: Sender<Sequenced<Resources>>,

    /// When the cgroup requests upscale it will send on this channel. We send
    /// an `UpscaleRequst` to the informant and then signal on the provided
    /// `oneshot::Sender` that we have acknowledged and processed the upscale
    /// request.
    pub(crate) request_upscale_events: Receiver<()>,

    /// The protocol version we have agreed to use with the informant. This is negotiated
    /// during the creation of the dispatcher, and should be the highest shared protocol
    /// version.
    ///
    // NOTE: currently unused, but will almost certainly be used in the futures
    // as the protocol changes
    #[allow(unused)]
    pub(crate) proto_version: ProtocolVersion,
}

impl Dispatcher {
    /// Creates a new dispatcher using the passed-in connection.
    ///
    /// Performs a negotiation with the informant to determine the highest protocol
    /// version that both support. This consists of two steps:
    /// 1. Wait for the informant to sent the range of protocols it supports.
    /// 2. Send a protocol version that works for us as well, or an error if there
    ///    is no compatible version.
    pub async fn new(
        stream: WebSocket,
        notify_upscale_events: Sender<Sequenced<Resources>>,
        request_upscale_events: Receiver<()>,
    ) -> anyhow::Result<Self> {
        let (mut sink, mut source) = stream.split();

        // Figure out the highest protocol version we both support
        info!("waiting for informant to send protocol version range");
        let Some(message) = source.next().await else {
            bail!("websocket connection closed while performing protocol handshake")
        };

        let message = message.context("failed to read protocol version range off connection")?;

        let Message::Text(message_text) = message else {
            // All messages should be in text form, since we don't do any
            // pinging/ponging. See nhooyr/websocket's implementation and the
            // informant/agent for more info
            bail!("received non-text message during proocol handshake: {message:?}")
        };

        let monitor_range = ProtocolRange {
            min: PROTOCOL_MIN_VERSION,
            max: PROTOCOL_MAX_VERSION,
        };

        let informant_range: ProtocolRange = serde_json::from_str(&message_text)
            .context("failed to deserialize protocol version range")?;

        info!(range = ?informant_range, "received protocol version range");

        let highest_shared_version = match monitor_range.highest_shared_version(&informant_range) {
            Ok(version) => {
                sink.send(Message::Text(
                    serde_json::to_string(&ProtocolResponse::Version(version)).unwrap(),
                ))
                .await
                .context("failed to notify informant of negotiated protocol version")?;
                version
            }
            Err(e) => {
                sink.send(Message::Text(
                    serde_json::to_string(&ProtocolResponse::Error(format!(
                        "Received protocol version range {} which does not overlap with {}",
                        informant_range, monitor_range
                    )))
                    .unwrap(),
                ))
                .await
                .context(
                    "failed to notify informant of no overlap between protocol version ranges",
                )?;
                Err(e).context("error determining suitable protocol version range")?
            }
        };

        Ok(Self {
            sink,
            source,
            notify_upscale_events,
            request_upscale_events,
            proto_version: highest_shared_version,
        })
    }

    /// Notify the cgroup manager that we have received upscale and wait for
    /// the acknowledgement.
    #[tracing::instrument(skip(self))]
    pub async fn notify_upscale(&self, resources: Sequenced<Resources>) -> anyhow::Result<()> {
        self.notify_upscale_events
            .send(resources)
            .await
            .context("failed to send resources and oneshot sender across channel")
    }

    /// Send a message to the informant.
    ///
    /// Although this function is small, it has one major benefit: it is the only
    /// way to send data accross the connection, and you can only pass in a proper
    /// `MonitorMessage`. Without safeguards like this, it's easy to accidentally
    /// serialize the wrong thing and send it, since `self.sink.send` will take
    /// any string.
    pub async fn send(&mut self, message: OutboundMsg) -> anyhow::Result<()> {
        info!(?message, "sending message");
        let json = serde_json::to_string(&message).context("failed to serialize message")?;
        self.sink
            .send(Message::Text(json))
            .await
            .context("stream error sending message")
    }
}
