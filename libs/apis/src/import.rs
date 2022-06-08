use std::path::PathBuf;

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

// TODO add a thin binary wrapper for this function, aside from neon_local
pub fn send_basebackup(basebackup_dir: PathBuf) -> anyhow::Result<()> {
    // TODO change return type
    // TODO implement as sender of ImportFeMessage and receiver of ImportBeMessage
    //      on generic channel.
    Ok(())
}
