use bytes::Bytes;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub enum ImportFeMessage {
    // TODO chunk it
    File(String, Bytes),
    Done
}

#[derive(Serialize, Deserialize)]
pub enum ImportBeMessage {
    Done,
    Error(String),
}
