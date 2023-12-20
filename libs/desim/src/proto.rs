use std::fmt::Debug;

use bytes::Bytes;
use utils::lsn::Lsn;

/// All possible flavours of messages.
/// Grouped by the receiver node.
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

#[derive(Clone, Debug)]
pub struct ReplCell {
    pub value: u32,
    pub client_id: u32,
    pub seqno: u32,
}
