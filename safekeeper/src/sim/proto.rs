/// All possible flavours of messages.
/// Grouped by the receiver node.
#[derive(Clone, Debug)]
pub enum AnyMessage {
    Just32(u32),
    ReplCell(ReplCell),
}

#[derive(Clone, Debug)]
pub struct ReplCell {
    pub value: u32,
    pub client_id: u32,
    pub seqno: u32,
}
