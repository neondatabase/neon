/// All possible flavours of messages.
/// Grouped by the receiver node.
#[derive(Clone, Debug)]
pub enum AnyMessage {
    Just32(u32),
}
