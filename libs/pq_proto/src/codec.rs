//! Provides `PostgresCodec` defining how to serilize/deserialize Postgres
//! messages to/from the wire, to be used with `tokio_util::codec::Framed`.
use std::io;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::{BeMessage, FeMessage, FeStartupPacket, ProtocolError};

// Defines how to serilize/deserialize Postgres messages to/from the wire, to be
// used with `tokio_util::codec::Framed`.
pub struct PostgresCodec {
    // Have we already decoded startup message? All further should start with
    // message type byte then.
    startup_read: bool,
}

impl PostgresCodec {
    pub fn new() -> Self {
        PostgresCodec {
            startup_read: false,
        }
    }
}

/// Error on postgres connection: either IO (physical transport error) or
/// protocol violation.
#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

impl Encoder<&BeMessage<'_>> for PostgresCodec {
    type Error = ConnectionError;

    fn encode(&mut self, item: &BeMessage, dst: &mut BytesMut) -> Result<(), ConnectionError> {
        BeMessage::write(dst, &item)?;
        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = FeMessage;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<FeMessage>, ConnectionError> {
        let msg = if !self.startup_read {
            let msg = FeStartupPacket::parse(src);
            if let Ok(Some(FeMessage::StartupPacket(FeStartupPacket::StartupMessage { .. }))) = msg
            {
                self.startup_read = true;
            }
            msg?
        } else {
            FeMessage::parse(src)?
        };
        Ok(msg)
    }
}
