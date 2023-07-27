use super::codec::{BackendMessages, FrontendMessage, PostgresCodec};
use super::error::Error;
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::{Sink, StreamExt};
use futures::{SinkExt, Stream};
use postgres_protocol::authentication;
use postgres_protocol::message::backend::{
    BackendKeyDataBody, DataRowBody, Message, ReadyForQueryBody, RowDescriptionBody,
};
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_native_tls::{native_tls, TlsConnector, TlsStream};
use tokio_postgres::maybe_tls_stream::MaybeTlsStream;
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

// enum MaybeTlsStream {
//     NoTls(TcpStream),
//     Tls(TlsStream<TcpStream>),
// }

// impl Unpin for MaybeTlsStream {}

// impl AsyncRead for MaybeTlsStream {
//     fn poll_read(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut tokio::io::ReadBuf<'_>,
//     ) -> Poll<std::io::Result<()>> {
//         match self.get_mut() {
//             MaybeTlsStream::NoTls(no_tls) => Pin::new(no_tls).poll_read(cx, buf),
//             MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_read(cx, buf),
//         }
//     }
// }
// impl AsyncWrite for MaybeTlsStream {
//     fn poll_write(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &[u8],
//     ) -> Poll<Result<usize, std::io::Error>> {
//         match self.get_mut() {
//             MaybeTlsStream::NoTls(no_tls) => Pin::new(no_tls).poll_write(cx, buf),
//             MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_write(cx, buf),
//         }
//     }

//     fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
//         match self.get_mut() {
//             MaybeTlsStream::NoTls(no_tls) => Pin::new(no_tls).poll_flush(cx),
//             MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_flush(cx),
//         }
//     }

//     fn poll_shutdown(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//     ) -> Poll<Result<(), std::io::Error>> {
//         match self.get_mut() {
//             MaybeTlsStream::NoTls(no_tls) => Pin::new(no_tls).poll_shutdown(cx),
//             MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_shutdown(cx),
//         }
//     }

//     fn poll_write_vectored(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         bufs: &[std::io::IoSlice<'_>],
//     ) -> Poll<Result<usize, std::io::Error>> {
//         match self.get_mut() {
//             MaybeTlsStream::NoTls(no_tls) => Pin::new(no_tls).poll_write_vectored(cx, bufs),
//             MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_write_vectored(cx, bufs),
//         }
//     }

//     fn is_write_vectored(&self) -> bool {
//         match self {
//             MaybeTlsStream::NoTls(no_tls) => no_tls.is_write_vectored(),
//             MaybeTlsStream::Tls(tls) => tls.is_write_vectored(),
//         }
//     }
// }

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> RawConnection<S, T> {
    // pub(crate) async fn connect(
    //     mut stream: TcpStream,
    //     tls_domain: Option<&str>,
    // ) -> Result<RawConnection<S, T>, Error> {
    //     let mut buf = BytesMut::new();

    //     let stream = if let Some(tls_domain) = tls_domain {
    //         frontend::ssl_request(&mut buf);
    //         stream
    //             .write_all_buf(&mut buf.split().freeze())
    //             .await
    //             .unwrap();
    //         let bit = stream.read_u8().await.map_err(Error::io)?;
    //         if bit != b'S' {
    //             return Err(Error::closed());
    //         }

    //         let tls = native_tls::TlsConnector::new().map_err(Error::tls)?;
    //         let tls = TlsConnector::from(tls)
    //             .connect(tls_domain, stream)
    //             .await
    //             .map_err(Error::tls)?;

    //         MaybeTlsStream::Tls(tls)
    //     } else {
    //         MaybeTlsStream::Raw(stream)
    //     };

    //     Ok(RawConnection::new(Framed::new(stream, PostgresCodec), buf))
    // }

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
    raw: RawConnection<S, T>,
    key: BackendKeyDataBody,
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> Connection<S, T> {
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

    pub fn prepare_and_execute(
        &mut self,
        portal: &str,
        name: &str,
        query: &str,
        params: impl IntoIterator<Item = Option<impl AsRef<str>>>,
    ) -> std::io::Result<()> {
        self.prepare(name, query)?;
        self.execute(portal, name, params)
    }

    pub fn prepare(&mut self, name: &str, query: &str) -> std::io::Result<()> {
        frontend::parse(name, query, std::iter::empty(), &mut self.raw.buf)
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
        frontend::describe(b'P', portal, &mut self.raw.buf)?;
        frontend::execute(portal, 0, &mut self.raw.buf)
    }

    pub async fn sync(&mut self) -> Result<(), Error> {
        frontend::sync(&mut self.raw.buf);
        self.raw.send().await
    }

    /// returns None if there's no row data
    /// returns Some with the row description and a row stream if there is row data
    pub async fn stream_query_results(
        &mut self,
    ) -> Result<
        Option<(
            RowDescriptionBody,
            impl Stream<Item = Result<DataRowBody, Error>> + '_,
        )>,
        Error,
    > {
        let Message::ParseComplete = self.raw.next_message().await? else { return Err(Error::expecting("parse")) };
        let Message::BindComplete = self.raw.next_message().await? else { return Err(Error::expecting("bind")) };
        match self.raw.next_message().await? {
            Message::RowDescription(desc) => {
                struct RowStream<'a, S, T> {
                    raw: &'a mut RawConnection<S, T>,
                }
                impl<S, T> Unpin for RowStream<'_, S, T> {}

                impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> Stream
                    for RowStream<'_, S, T>
                {
                    type Item = Result<DataRowBody, Error>;

                    fn poll_next(
                        mut self: Pin<&mut Self>,
                        cx: &mut Context<'_>,
                    ) -> Poll<Option<Self::Item>> {
                        match ready!(self.raw.poll_read(cx)?) {
                            Message::DataRow(row) => Poll::Ready(Some(Ok(row))),
                            Message::CommandComplete(_) => Poll::Ready(None),
                            _ => Poll::Ready(Some(Err(Error::expecting("command completion")))),
                        }
                    }
                }

                Ok(Some((desc, RowStream { raw: &mut self.raw })))
            }
            Message::NoData => {
                let Message::CommandComplete(_) = self.raw.next_message().await? else { return Err(Error::expecting("command completion")) };
                Ok(None)
            }
            _ => Err(Error::expecting("query results")),
        }
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
