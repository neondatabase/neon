use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{Sink, StreamExt, ready};
use postgres_protocol2::message::backend::{Message, NoticeResponseBody};
use postgres_protocol2::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::sync::PollSender;
use tracing::trace;

use crate::Error;
use crate::codec::{
    BackendMessage, BackendMessages, FrontendMessage, PostgresCodec, RecordNotices,
};
use crate::maybe_tls_stream::MaybeTlsStream;

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
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,

    sender: PollSender<BackendMessages>,
    receiver: mpsc::UnboundedReceiver<FrontendMessage>,
    notices: Option<RecordNotices>,

    pending_response: Option<BackendMessages>,
    state: State,
}

pub const INITIAL_CAPACITY: usize = 2 * 1024;

/// Gargabe collect the [`BytesMut`] if it has too much spare capacity.
pub fn gc_bytesmut(buf: &mut BytesMut) {
    const GC_THRESHOLD: usize = 16 * 1024;

    // We use a different mode to shrink the buf when above the threshold.
    // When above the threshold, we only re-allocate when the buf has 2x spare capacity.
    let reclaim = GC_THRESHOLD.checked_sub(buf.len()).unwrap_or(buf.len());

    // `try_reclaim` tries to get the capacity from any shared `BytesMut`s,
    // before then comparing the length against the capacity.
    if buf.try_reclaim(reclaim) {
        let capacity = usize::max(buf.len(), INITIAL_CAPACITY);

        // Allocate a new `BytesMut` so that we deallocate the old version.
        let mut new = BytesMut::with_capacity(capacity);
        new.extend_from_slice(buf);
        *buf = new;
    }
}

pub enum Never {}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        sender: mpsc::Sender<BackendMessages>,
        receiver: mpsc::UnboundedReceiver<FrontendMessage>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            sender: PollSender::new(sender),
            receiver,
            notices: None,
            pending_response: None,
            state: State::Active,
        }
    }

    /// Read and process messages from the connection to postgres.
    /// client <- postgres
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<Never, Error>> {
        loop {
            let messages = match self.pending_response.take() {
                Some(messages) => messages,
                None => {
                    let message = match self.stream.poll_next_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => return Poll::Ready(Err(Error::closed())),
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(Error::io(e))),
                        Poll::Ready(Some(Ok(message))) => message,
                    };

                    match message {
                        BackendMessage::Async(Message::NoticeResponse(body)) => {
                            self.handle_notice(body)?;
                            continue;
                        }
                        BackendMessage::Async(_) => continue,
                        BackendMessage::Normal { messages, ready } => {
                            // if we read a ReadyForQuery from postgres, let's try GC the read buffer.
                            if ready {
                                gc_bytesmut(self.stream.read_buffer_mut());
                            }

                            messages
                        }
                    }
                }
            };

            match self.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    let _ = self.sender.send_item(messages);
                }
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(Error::closed()));
                }
                Poll::Pending => {
                    self.pending_response = Some(messages);
                    trace!("poll_read: waiting on sender");
                    return Poll::Pending;
                }
            }
        }
    }

    fn handle_notice(&mut self, body: NoticeResponseBody) -> Result<(), Error> {
        let Some(notices) = &mut self.notices else {
            return Ok(());
        };

        let mut fields = body.fields();
        while let Some(field) = fields.next().map_err(Error::parse)? {
            // loop until we find the message field
            if field.type_() == b'M' {
                // if the message field is within the limit, send it.
                if let Some(new_limit) = notices.limit.checked_sub(field.value().len()) {
                    match notices.sender.send(field.value().into()) {
                        // set the new limit.
                        Ok(()) => notices.limit = new_limit,
                        // closed.
                        Err(_) => self.notices = None,
                    }
                }
                break;
            }
        }

        Ok(())
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
                Poll::Ready(Some(FrontendMessage::Raw(request))) => {
                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;
                }
                Poll::Ready(Some(FrontendMessage::RecordNotices(notices))) => {
                    self.notices = Some(notices)
                }
                // No more messages from the client, and no more responses to wait for.
                // Send a terminate message to postgres
                Poll::Ready(None) => {
                    trace!("poll_write: at eof, terminating");
                    frontend::terminate(self.stream.write_buffer_mut());

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

                // GC the write buffer if we managed to flush
                gc_bytesmut(self.stream.write_buffer_mut());

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

    fn poll_message(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Never, Error>>> {
        if self.state != State::Closing {
            // if the state is still active, try read from and write to postgres.
            let Poll::Pending = self.poll_read(cx)?;
            if self.poll_write(cx)?.is_ready() {
                self.state = State::Closing;
            }

            // poll_read returned Pending.
            // poll_write returned Pending or Ready(()).
            // if poll_write returned Ready(()), then we are waiting to read more data from postgres.
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
        match self.poll_message(cx)? {
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}
