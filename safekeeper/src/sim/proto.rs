/// All possible flavours of messages.
/// Grouped by the receiver node.
#[derive(Clone)]
pub enum AnyMessage {
    Just32(u32),
}
