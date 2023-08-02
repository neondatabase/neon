use super::codec::{BackendMessages, FrontendMessage, PostgresCodec};
use super::error::Error;
use super::prepare::TypeinfoPreparedQueries;
use bytes::{BufMut, BytesMut};
use futures::channel::mpsc;
use futures::{Sink, StreamExt};
use futures::{SinkExt, Stream};
use hashbrown::HashMap;
use postgres_protocol::message::backend::{
    BackendKeyDataBody, CommandCompleteBody, DataRowBody, ErrorResponseBody, Message,
    ReadyForQueryBody, RowDescriptionBody,
};
use postgres_protocol::message::frontend;
use postgres_protocol::Oid;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::maybe_tls_stream::MaybeTlsStream;
use tokio_postgres::types::Type;
use tokio_postgres::IsolationLevel;
use tokio_util::codec::Framed;

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

/// A connection to a PostgreSQL database.
pub struct RawConnection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    pending_responses: VecDeque<Message>,
    pub buf: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> RawConnection<S, T> {
    pub fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        buf: BytesMut,
    ) -> RawConnection<S, T> {
        RawConnection {
            stream,
            pending_responses: VecDeque::new(),
            buf,
        }
    }

    pub async fn send(&mut self) -> Result<(), Error> {
        poll_fn(|cx| self.poll_send(cx)).await?;
        let request = FrontendMessage(self.buf.split().freeze());
        self.stream.start_send_unpin(request).map_err(Error::io)?;
        poll_fn(|cx| self.poll_flush(cx)).await
    }

    pub async fn next_message(&mut self) -> Result<Message, Error> {
        match self.pending_responses.pop_front() {
            Some(message) => Ok(message),
            None => poll_fn(|cx| self.poll_read(cx)).await,
        }
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<Message, Error>> {
        let message = match ready!(self.stream.poll_next_unpin(cx)?) {
            Some(message) => message,
            None => return Poll::Ready(Err(Error::closed())),
        };
        Poll::Ready(Ok(message))
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_close(cx).map_err(Error::io)
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

pub struct Connection<S, T> {
    stmt_counter: usize,
    pub typeinfo: Option<TypeinfoPreparedQueries>,
    pub typecache: HashMap<Oid, Type>,
    pub raw: RawConnection<S, T>,
    // key: BackendKeyDataBody,
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> Connection<S, T> {
    pub fn new(stream: MaybeTlsStream<S, T>) -> Connection<S, T> {
        Connection {
            stmt_counter: 0,
            typeinfo: None,
            typecache: HashMap::new(),
            raw: RawConnection::new(Framed::new(stream, PostgresCodec), BytesMut::new()),
        }
    }

    pub async fn start_tx(
        &mut self,
        isolation_level: Option<IsolationLevel>,
        read_only: Option<bool>,
    ) -> Result<ReadyForQueryBody, Error> {
        let mut query = "START TRANSACTION".to_string();
        let mut first = true;

        if let Some(level) = isolation_level {
            first = false;

            query.push_str(" ISOLATION LEVEL ");
            let level = match level {
                IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
                IsolationLevel::ReadCommitted => "READ COMMITTED",
                IsolationLevel::RepeatableRead => "REPEATABLE READ",
                IsolationLevel::Serializable => "SERIALIZABLE",
                _ => return Err(Error::unexpected_message()),
            };
            query.push_str(level);
        }

        if let Some(read_only) = read_only {
            if !first {
                query.push(',');
            }
            first = false;

            let s = if read_only {
                " READ ONLY"
            } else {
                " READ WRITE"
            };
            query.push_str(s);
        }

        self.execute_simple(&query).await
    }

    pub async fn rollback(&mut self) -> Result<ReadyForQueryBody, Error> {
        self.execute_simple("ROLLBACK").await
    }

    pub async fn commit(&mut self) -> Result<ReadyForQueryBody, Error> {
        self.execute_simple("COMMIT").await
    }

    // pub async fn auth_sasl_scram<'a, I>(
    //     mut raw: RawConnection<S, T>,
    //     params: I,
    //     password: &[u8],
    // ) -> Result<Self, Error>
    // where
    //     I: IntoIterator<Item = (&'a str, &'a str)>,
    // {
    //     // send a startup message
    //     frontend::startup_message(params, &mut raw.buf).unwrap();
    //     raw.send().await?;

    //     // expect sasl authentication message
    //     let Message::AuthenticationSasl(body) = raw.next_message().await? else { return Err(Error::expecting("sasl authentication")) };
    //     // expect support for SCRAM_SHA_256
    //     if body
    //         .mechanisms()
    //         .find(|&x| Ok(x == authentication::sasl::SCRAM_SHA_256))?
    //         .is_none()
    //     {
    //         return Err(Error::expecting("SCRAM-SHA-256 auth"));
    //     }

    //     // initiate SCRAM_SHA_256 authentication without channel binding
    //     let auth = authentication::sasl::ChannelBinding::unrequested();
    //     let mut scram = authentication::sasl::ScramSha256::new(password, auth);

    //     frontend::sasl_initial_response(
    //         authentication::sasl::SCRAM_SHA_256,
    //         scram.message(),
    //         &mut raw.buf,
    //     )
    //     .unwrap();
    //     raw.send().await?;

    //     // expect sasl continue
    //     let Message::AuthenticationSaslContinue(b) = raw.next_message().await? else { return Err(Error::expecting("auth continue")) };
    //     scram.update(b.data()).unwrap();

    //     // continue sasl
    //     frontend::sasl_response(scram.message(), &mut raw.buf).unwrap();
    //     raw.send().await?;

    //     // expect sasl final
    //     let Message::AuthenticationSaslFinal(b) = raw.next_message().await? else { return Err(Error::expecting("auth final")) };
    //     scram.finish(b.data()).unwrap();

    //     // expect auth ok
    //     let Message::AuthenticationOk = raw.next_message().await? else { return Err(Error::expecting("auth ok")) };

    //     // expect connection accepted
    //     let key = loop {
    //         match raw.next_message().await? {
    //             Message::BackendKeyData(key) => break key,
    //             Message::ParameterStatus(_) => {}
    //             _ => return Err(Error::expecting("backend ready")),
    //         }
    //     };

    //     let Message::ReadyForQuery(b) = raw.next_message().await? else { return Err(Error::expecting("ready for query")) };
    //     // assert_eq!(b.status(), b'I');

    //     Ok(Self { raw, key })
    // }

    // pub fn prepare_and_execute(
    //     &mut self,
    //     portal: &str,
    //     name: &str,
    //     query: &str,
    //     params: impl IntoIterator<Item = Option<impl AsRef<str>>>,
    // ) -> std::io::Result<()> {
    //     self.prepare(name, query)?;
    //     self.execute(portal, name, params)
    // }

    pub fn statement_name(&mut self) -> String {
        self.stmt_counter += 1;
        format!("s{}", self.stmt_counter)
    }

    async fn execute_simple(&mut self, query: &str) -> Result<ReadyForQueryBody, Error> {
        frontend::query(query, &mut self.raw.buf)?;
        self.raw.send().await?;

        loop {
            match self.raw.next_message().await? {
                Message::ReadyForQuery(q) => return Ok(q),
                Message::CommandComplete(_)
                | Message::EmptyQueryResponse
                | Message::RowDescription(_)
                | Message::DataRow(_) => {}
                _ => return Err(Error::unexpected_message()),
            }
        }
    }

    pub async fn prepare(&mut self, name: &str, query: &str) -> Result<RowDescriptionBody, Error> {
        frontend::parse(name, query, std::iter::empty(), &mut self.raw.buf)?;
        frontend::describe(b'S', name, &mut self.raw.buf)?;
        self.sync().await?;
        self.wait_for_prepare().await
    }

    pub fn execute(
        &mut self,
        portal: &str,
        name: &str,
        params: impl IntoIterator<Item = Option<impl AsRef<str>>>,
    ) -> std::io::Result<()> {
        frontend::bind(
            portal,
            name,
            std::iter::empty(), // all parameters use the default format (text)
            params,
            |param, buf| match param {
                Some(param) => {
                    buf.put_slice(param.as_ref().as_bytes());
                    Ok(postgres_protocol::IsNull::No)
                }
                None => Ok(postgres_protocol::IsNull::Yes),
            },
            Some(0), // all text
            &mut self.raw.buf,
        )
        .map_err(|e| match e {
            frontend::BindError::Conversion(e) => std::io::Error::new(std::io::ErrorKind::Other, e),
            frontend::BindError::Serialization(io) => io,
        })?;
        frontend::execute(portal, 0, &mut self.raw.buf)
    }

    pub async fn sync(&mut self) -> Result<(), Error> {
        frontend::sync(&mut self.raw.buf);
        self.raw.send().await
    }

    pub async fn wait_for_prepare(&mut self) -> Result<RowDescriptionBody, Error> {
        let Message::ParseComplete = self.raw.next_message().await? else { return Err(Error::expecting("parse")) };
        let Message::ParameterDescription(_) = self.raw.next_message().await? else { return Err(Error::expecting("param description")) };
        let Message::RowDescription(desc) = self.raw.next_message().await? else { return Err(Error::expecting("row description")) };

        self.wait_for_ready().await?;

        Ok(desc)
    }

    pub async fn stream_query_results(&mut self) -> Result<RowStream<'_, S, T>, Error> {
        // let Message::ParseComplete = self.raw.next_message().await? else { return Err(Error::expecting("parse")) };
        let Message::BindComplete = self.raw.next_message().await? else { return Err(Error::expecting("bind")) };
        Ok(RowStream::Stream(&mut self.raw))
    }

    pub async fn wait_for_ready(&mut self) -> Result<ReadyForQueryBody, Error> {
        loop {
            match self.raw.next_message().await.unwrap() {
                Message::ReadyForQuery(b) => break Ok(b),
                _ => continue,
            }
        }
    }
}

pub enum RowStream<'a, S, T> {
    Stream(&'a mut RawConnection<S, T>),
    Complete(Option<CommandCompleteBody>),
}
impl<S, T> Unpin for RowStream<'_, S, T> {}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> Stream
    for RowStream<'_, S, T>
{
    // this is horrible - first result is for transport/protocol errors errors
    // second result is for sql errors.
    type Item = Result<Result<DataRowBody, ErrorResponseBody>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            RowStream::Stream(raw) => match ready!(raw.poll_read(cx)?) {
                Message::DataRow(row) => Poll::Ready(Some(Ok(Ok(row)))),
                Message::CommandComplete(tag) => {
                    *self = Self::Complete(Some(tag));
                    Poll::Ready(None)
                }
                Message::EmptyQueryResponse | Message::PortalSuspended => {
                    *self = Self::Complete(None);
                    Poll::Ready(None)
                }
                Message::ErrorResponse(error) => {
                    *self = Self::Complete(None);
                    Poll::Ready(Some(Ok(Err(error))))
                }
                _ => Poll::Ready(Some(Err(Error::expecting("command completion")))),
            },
            RowStream::Complete(_) => Poll::Ready(None),
        }
    }
}

impl<S, T> RowStream<'_, S, T> {
    pub fn tag(self) -> Option<CommandCompleteBody> {
        match self {
            RowStream::Stream(_) => panic!("should not get tag unless row stream is exhausted"),
            RowStream::Complete(tag) => tag,
        }
    }
}
