use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures_util::{Sink, Stream, ready};
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::sync::PollSender;
use tracing::{info, trace};

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::error::DbError;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{AsyncMessage, Error, Notification};

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Closing,
}

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

    sender: PollSender<BackendMessages>,
    receiver: mpsc::UnboundedReceiver<FrontendMessage>,

    pending_responses: VecDeque<BackendMessage>,
    state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        pending_responses: VecDeque<BackendMessage>,
        parameters: HashMap<String, String>,
        sender: mpsc::Sender<BackendMessages>,
        receiver: mpsc::UnboundedReceiver<FrontendMessage>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            sender: PollSender::new(sender),
            receiver,
            pending_responses,
            state: State::Active,
        }
    }

    fn poll_response(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BackendMessage, Error>>> {
        if let Some(message) = self.pending_responses.pop_front() {
            trace!("retrying pending response");
            return Poll::Ready(Some(Ok(message)));
        }

        Pin::new(&mut self.stream)
            .poll_next(cx)
            .map(|o| o.map(|r| r.map_err(Error::io)))
    }

    /// Read and process messages from the connection to postgres.
    /// client <- postgres
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<AsyncMessage, Error>> {
        loop {
            let message = match self.poll_response(cx)? {
                Poll::Ready(Some(message)) => message,
                Poll::Ready(None) => return Poll::Ready(Err(Error::closed())),
                Poll::Pending => {
                    trace!("poll_read: waiting on response");
                    return Poll::Pending;
                }
            };

            let messages = match message {
                BackendMessage::Async(Message::NoticeResponse(body)) => {
                    let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    return Poll::Ready(Ok(AsyncMessage::Notice(error)));
                }
                BackendMessage::Async(Message::NotificationResponse(body)) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    };
                    return Poll::Ready(Ok(AsyncMessage::Notification(notification)));
                }
                BackendMessage::Async(Message::ParameterStatus(body)) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                BackendMessage::Async(_) => unreachable!(),
                BackendMessage::Normal { messages } => messages,
            };

            match self.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    let _ = self.sender.send_item(messages);
                }
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(Error::closed()));
                }
                Poll::Pending => {
                    self.pending_responses
                        .push_back(BackendMessage::Normal { messages });
                    trace!("poll_read: waiting on sender");
                    return Poll::Pending;
                }
            }
        }
    }

    /// Fetch the next client request and enqueue the response sender.
    fn poll_request(&mut self, cx: &mut Context<'_>) -> Poll<Option<FrontendMessage>> {
        if self.receiver.is_closed() {
            return Poll::Ready(None);
        }

        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(request)) => {
                trace!("polled new request");
                Poll::Ready(Some(request))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Process client requests and write them to the postgres connection, flushing if necessary.
    /// client -> postgres
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            if Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
                .is_pending()
            {
                trace!("poll_write: waiting on socket");

                // poll_ready is self-flushing.
                return Poll::Pending;
            }

            match self.poll_request(cx) {
                // send the message to postgres
                Poll::Ready(Some(request)) => {
                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;
                }
                // No more messages from the client, and no more responses to wait for.
                // Send a terminate message to postgres
                Poll::Ready(None) => {
                    trace!("poll_write: at eof, terminating");
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    let request = FrontendMessage::Raw(request.freeze());

                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;

                    trace!("poll_write: sent eof, closing");
                    trace!("poll_write: done");
                    return Poll::Ready(Ok(()));
                }
                // Still waiting for a message from the client.
                Poll::Pending => {
                    trace!("poll_write: waiting on request");
                    ready!(self.poll_flush(cx))?;
                    return Poll::Pending;
                }
            }
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match Pin::new(&mut self.stream)
            .poll_flush(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => {
                trace!("poll_flush: flushed");
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                trace!("poll_flush: waiting on socket");
                Poll::Pending
            }
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

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        if self.state != State::Closing {
            // if the state is still active, try read from and write to postgres.
            let message = self.poll_read(cx)?;
            let closing = self.poll_write(cx)?;
            if let Poll::Ready(()) = closing {
                self.state = State::Closing;
            }

            if let Poll::Ready(message) = message {
                return Poll::Ready(Some(Ok(message)));
            }

            // poll_read returned Pending.
            // poll_write returned Pending or Ready(WriteReady::WaitingOnRead).
            // if poll_write returned Ready(WriteReady::WaitingOnRead), then we are waiting to read more data from postgres.
            if self.state != State::Closing {
                return Poll::Pending;
            }
        }

        match self.poll_shutdown(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(None),
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(message) = ready!(self.poll_message(cx)?) {
            if let AsyncMessage::Notice(notice) = message {
                info!("{}: {}", notice.severity(), notice.message());
            }
        }
        Poll::Ready(Ok(()))
    }
}
