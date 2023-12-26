use std::fmt::Debug;

use bytes::Bytes;
use utils::lsn::Lsn;

use crate::{network::TCP, world::NodeId};

/// Internal node events.
#[derive(Debug)]
pub enum NodeEvent {
    Accept(TCP),
    Internal(AnyMessage),
}

/// Events that are coming from a network socket.
#[derive(Clone, Debug)]
pub enum NetEvent {
    Message(AnyMessage),
    Closed,
}

/// Custom events generated throughout the simulation. Can be used by the test to verify the correctness.
#[derive(Debug)]
pub struct SimEvent {
    pub time: u64,
    pub node: NodeId,
    pub data: String,
}

/// Umbrella type for all possible flavours of messages. These events can be sent over network
/// or to an internal node events channel.
#[derive(Clone)]
pub enum AnyMessage {
    /// Not used, empty placeholder.
    None,
    /// Used internally for notifying node about new incoming connection.
    InternalConnect,
    Just32(u32),
    ReplCell(ReplCell),
    Bytes(Bytes),
    LSN(u64),
}

impl Debug for AnyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyMessage::None => write!(f, "None"),
            AnyMessage::InternalConnect => write!(f, "InternalConnect"),
            AnyMessage::Just32(v) => write!(f, "Just32({})", v),
            AnyMessage::ReplCell(v) => write!(f, "ReplCell({:?})", v),
            AnyMessage::Bytes(v) => write!(f, "Bytes({})", hex::encode(v)),
            AnyMessage::LSN(v) => write!(f, "LSN({})", Lsn(*v)),
        }
    }
}

/// Used in reliable_copy_test.rs
#[derive(Clone, Debug)]
pub struct ReplCell {
    pub value: u32,
    pub client_id: u32,
    pub seqno: u32,
}
