use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{TryStreamExt, future, ready};
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::codec::{BackendMessages, FrontendMessage};
use crate::config::{Host, SslMode};
use crate::query::RowStream;
use crate::simple_query::SimpleQueryStream;
use crate::types::{Oid, Type};
use crate::{
    CancelToken, Error, ReadyForQueryStatus, SimpleQueryMessage, Statement, Transaction,
    TransactionBuilder, query, simple_query,
};

pub struct Responses {
    /// new messages from conn
    receiver: mpsc::Receiver<BackendMessages>,
    /// current batch of messages
    cur: BackendMessages,
    /// number of total queries sent.
    waiting: usize,
    /// number of ReadyForQuery messages received.
    received: usize,
}

impl Responses {
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Message, Error>> {
        loop {
            // get the next saved message
            if let Some(message) = self.cur.next().map_err(Error::parse)? {
                let received = self.received;

                // increase the query head if this is the last message.
                if let Message::ReadyForQuery(_) = message {
                    self.received += 1;
                }

                // check if the client has skipped this query.
                if received + 1 < self.waiting {
                    // grab the next message.
                    continue;
                }

                // convenience: turn the error messaage into a proper error.
                let res = match message {
                    Message::ErrorResponse(body) => Err(Error::db(body)),
                    message => Ok(message),
                };
                return Poll::Ready(res);
            }

            // get the next back of messages.
            match ready!(self.receiver.poll_recv(cx)) {
                Some(messages) => self.cur = messages,
                None => return Poll::Ready(Err(Error::closed())),
            }
        }
    }

    pub async fn next(&mut self) -> Result<Message, Error> {
        future::poll_fn(|cx| self.poll_next(cx)).await
    }
}

/// A cache of type info and prepared statements for fetching type info
/// (corresponding to the queries in the [crate::prepare] module).
#[derive(Default)]
pub(crate) struct CachedTypeInfo {
    /// A statement for basic information for a type from its
    /// OID. Corresponds to [TYPEINFO_QUERY](crate::prepare::TYPEINFO_QUERY) (or its
    /// fallback).
    pub(crate) typeinfo: Option<Statement>,

    /// Cache of types already looked up.
    pub(crate) types: HashMap<Oid, Type>,
}

pub struct InnerClient {
    sender: mpsc::UnboundedSender<FrontendMessage>,
    responses: Responses,

    /// A buffer to use when writing out postgres commands.
    buffer: BytesMut,
}

impl InnerClient {
    pub fn send(&mut self, messages: FrontendMessage) -> Result<&mut Responses, Error> {
        self.sender.send(messages).map_err(|_| Error::closed())?;
        self.responses.waiting += 1;
        Ok(&mut self.responses)
    }

    /// Call the given function with a buffer to be used when writing out
    /// postgres commands.
    pub fn with_buf<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let r = f(&mut self.buffer);
        self.buffer.clear();
        r
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SocketConfig {
    pub host_addr: Option<IpAddr>,
    pub host: Host,
    pub port: u16,
    pub connect_timeout: Option<Duration>,
}

/// An asynchronous PostgreSQL client.
///
/// The client is one half of what is returned when a connection is established. Users interact with the database
/// through this client object.
pub struct Client {
    inner: InnerClient,
    cached_typeinfo: CachedTypeInfo,

    socket_config: SocketConfig,
    ssl_mode: SslMode,
    process_id: i32,
    secret_key: i32,
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(stmt) = self.cached_typeinfo.typeinfo.take() {
            let buf = self.inner.with_buf(|buf| {
                frontend::close(b'S', stmt.name(), buf).unwrap();
                frontend::sync(buf);
                buf.split().freeze()
            });
            let _ = self.inner.send(FrontendMessage::Raw(buf));
        }
    }
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<FrontendMessage>,
        receiver: mpsc::Receiver<BackendMessages>,
        socket_config: SocketConfig,
        ssl_mode: SslMode,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            inner: InnerClient {
                sender,
                responses: Responses {
                    receiver,
                    cur: BackendMessages::empty(),
                    waiting: 0,
                    received: 0,
                },
                buffer: Default::default(),
            },
            cached_typeinfo: Default::default(),

            socket_config,
            ssl_mode,
            process_id,
            secret_key,
        }
    }

    /// Returns process_id.
    pub fn get_process_id(&self) -> i32 {
        self.process_id
    }

    pub(crate) fn inner(&mut self) -> &mut InnerClient {
        &mut self.inner
    }

    /// Pass text directly to the Postgres backend to allow it to sort out typing itself and
    /// to save a roundtrip
    pub async fn query_raw_txt<S, I>(
        &mut self,
        statement: &str,
        params: I,
    ) -> Result<RowStream, Error>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = Option<S>>,
        I::IntoIter: ExactSizeIterator,
    {
        query::query_txt(&mut self.inner, statement, params).await
    }

    /// Executes a sequence of SQL statements using the simple query protocol, returning the resulting rows.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a list of the
    /// rows, this method returns a list of an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub async fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_raw(query).await?.try_collect().await
    }

    pub(crate) async fn simple_query_raw(
        &mut self,
        query: &str,
    ) -> Result<SimpleQueryStream, Error> {
        simple_query::simple_query(self.inner(), query).await
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for use when, for example, initializing a database schema.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub async fn batch_execute(&mut self, query: &str) -> Result<ReadyForQueryStatus, Error> {
        simple_query::batch_execute(self.inner(), query).await
    }

    pub async fn discard_all(&mut self) -> Result<ReadyForQueryStatus, Error> {
        // clear the prepared statements that are about to be nuked from the postgres session

        self.cached_typeinfo.typeinfo = None;

        self.batch_execute("discard all").await
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        struct RollbackIfNotDone<'me> {
            client: &'me mut Client,
            done: bool,
        }

        impl Drop for RollbackIfNotDone<'_> {
            fn drop(&mut self) {
                if self.done {
                    return;
                }

                let buf = self.client.inner().with_buf(|buf| {
                    frontend::query("ROLLBACK", buf).unwrap();
                    buf.split().freeze()
                });
                let _ = self.client.inner().send(FrontendMessage::Raw(buf));
            }
        }

        // This is done, as `Future` created by this method can be dropped after
        // `RequestMessages` is synchronously send to the `Connection` by
        // `batch_execute()`, but before `Responses` is asynchronously polled to
        // completion. In that case `Transaction` won't be created and thus
        // won't be rolled back.
        {
            let mut cleaner = RollbackIfNotDone {
                client: self,
                done: false,
            };
            cleaner.client.batch_execute("BEGIN").await?;
            cleaner.done = true;
        }

        Ok(Transaction::new(self))
    }

    /// Returns a builder for a transaction with custom settings.
    ///
    /// Unlike the `transaction` method, the builder can be used to control the transaction's isolation level and other
    /// attributes.
    pub fn build_transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder::new(self)
    }

    /// Constructs a cancellation token that can later be used to request cancellation of a query running on the
    /// connection associated with this client.
    pub fn cancel_token(&self) -> CancelToken {
        CancelToken {
            socket_config: Some(self.socket_config.clone()),
            ssl_mode: self.ssl_mode,
            process_id: self.process_id,
            secret_key: self.secret_key,
        }
    }

    /// Query for type information
    pub(crate) async fn get_type_inner(&mut self, oid: Oid) -> Result<Type, Error> {
        crate::prepare::get_type(&mut self.inner, &mut self.cached_typeinfo, oid).await
    }

    /// Determines if the connection to the server has already closed.
    ///
    /// In that case, all future queries will fail.
    pub fn is_closed(&self) -> bool {
        self.inner.sender.is_closed()
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish()
    }
}
