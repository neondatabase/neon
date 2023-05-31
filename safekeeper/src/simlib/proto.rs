/// All possible flavours of messages.
/// Grouped by the receiver node.
#[derive(Clone, Debug)]
#[repr(C)]
pub enum AnyMessage {
    /// Used internally for notifying node about new incoming connection.
    InternalConnect,
    Just32(u32),
    ReplCell(ReplCell),
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct ReplCell {
    pub value: u32,
    pub client_id: u32,
    pub seqno: u32,
}
