use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol2::message::backend;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{Decoder, Encoder};

pub enum FrontendMessage {
    Raw(BytesMut),
    RecordNotices(RecordNotices),
}

pub struct RecordNotices {
    pub sender: UnboundedSender<Box<str>>,
    pub limit: usize,
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        ready: bool,
    },
    Async(backend::Message),
}

pub struct BackendMessages(BytesMut);

impl BackendMessages {
    pub fn empty() -> BackendMessages {
        BackendMessages(BytesMut::new())
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(&mut self.0)
    }
}

pub struct PostgresCodec;

impl Encoder<BytesMut> for PostgresCodec {
    type Error = io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> io::Result<()> {
        // When it comes to request/response workflows, we usually flush the entire write
        // buffer in order to wait for the response before we send a new request.
        // Therefore we can avoid the copy and just replace the buffer.
        if dst.is_empty() {
            *dst = item;
        } else {
            dst.extend_from_slice(&item);
        }
        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;

        let mut ready = false;
        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let message = backend::Message::parse(src)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                ready = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(BackendMessage::Normal {
                messages: BackendMessages(src.split_to(idx)),
                ready,
            }))
        }
    }
}
