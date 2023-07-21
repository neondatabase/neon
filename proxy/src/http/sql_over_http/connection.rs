use super::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use super::error::Error;
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::SinkExt;
use futures::{Sink, StreamExt};
use postgres_protocol::message::backend::Message;
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::maybe_tls_stream::MaybeTlsStream;
use tokio_util::codec::Framed;
use tracing::trace;

pub enum RequestMessages {
    Single(FrontendMessage),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Response {
    sender: mpsc::Sender<BackendMessages>,
}

// #[derive(PartialEq, Debug)]
// enum State {
//     Active,
//     Terminating,
//     Closing,
// }

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T> {
    /// HACK: we need this in the Neon Proxy.
    pub stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    /// HACK: we need this in the Neon Proxy to forward params.
    pub parameters: HashMap<String, String>,
    // receiver: mpsc::UnboundedReceiver<Request>,
    pending_request: Option<RequestMessages>,
    pending_responses: VecDeque<(BackendMessages, bool)>,
    pub buf: BytesMut,
    // responses: VecDeque<Response>,
    // state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        pending_responses: VecDeque<(BackendMessages, bool)>,
        parameters: HashMap<String, String>,
        // receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            // receiver,
            pending_request: None,
            pending_responses,
            buf: BytesMut::new(),
            // responses: VecDeque::new(),
            // state: State::Active,
        }
    }

    pub async fn send(&mut self) -> Result<(), Error> {
        poll_fn(|cx| self.poll_send(cx)).await?;
        let request = FrontendMessage::Raw(self.buf.split().freeze());
        self.stream.start_send_unpin(request).map_err(Error::io)
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        poll_fn(|cx| self.poll_flush(cx)).await
    }

    pub async fn next_response(&mut self) -> Result<(BackendMessages, bool), Error> {
        match self.pending_responses.pop_front() {
            Some((a, b)) => Ok((a, b)),
            None => poll_fn(|cx| self.poll_read(cx)).await,
        }
    }

    pub async fn next_message(&mut self) -> Result<Message, Error> {
        loop {
            let (mut messages, complete) = self.next_response().await?;
            if let Some(message) = messages.next().map_err(Error::parse)? {
                self.pending_responses.push_front((messages, complete));
                break Ok(message);
            }
            if complete {
                break Err(Error::unexpected_message());
            }
        }
    }

    fn poll_response(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BackendMessage, Error>>> {
        self.stream
            .poll_next_unpin(cx)
            .map(|o| o.map(|r| r.map_err(Error::io)))
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<(BackendMessages, bool), Error>> {
        loop {
            let message = match ready!(self.poll_response(cx)?) {
                Some(message) => message,
                None => return Poll::Ready(Err(Error::closed())),
            };

            match message {
                BackendMessage::Async(Message::NoticeResponse(body)) => {
                    // TODO: log this

                    // let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    // return Ok(Some(AsyncMessage::Notice(error)));
                    continue;
                }
                BackendMessage::Async(Message::NotificationResponse(body)) => {
                    // TODO: log this

                    // let notification = Notification {
                    //     process_id: body.process_id(),
                    //     channel: body.channel().map_err(Error::parse)?.to_string(),
                    //     payload: body.message().map_err(Error::parse)?.to_string(),
                    // };
                    // return Ok(Some(AsyncMessage::Notification(notification)));
                    continue;
                }
                BackendMessage::Async(Message::ParameterStatus(body)) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                BackendMessage::Async(_) => unreachable!(),
                BackendMessage::Normal {
                    messages,
                    request_complete,
                } => return Poll::Ready(Ok((messages, request_complete))),
            };
        }
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match Pin::new(&mut self.stream)
            .poll_close(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => {
                trace!("poll_shutdown: complete");
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                trace!("poll_shutdown: waiting on socket");
                Poll::Pending
            }
        }
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if let Poll::Ready(msg) = self.poll_read(cx)? {
            self.pending_responses.push_back(msg);
        };
        self.stream.poll_ready_unpin(cx).map_err(Error::io)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if let Poll::Ready(msg) = self.poll_read(cx)? {
            self.pending_responses.push_back(msg);
        };
        self.stream.poll_flush_unpin(cx).map_err(Error::io)
    }
}
