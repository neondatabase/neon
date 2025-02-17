use crate::codec::{BackendMessages, FrontendMessage};

use crate::config::Host;
use crate::config::SslMode;
use crate::connection::{Request, RequestMessages};

use crate::query::RowStream;
use crate::simple_query::SimpleQueryStream;

use crate::{
    query, simple_query, CancelToken, Error, ReadyForQueryStatus, SimpleQueryMessage, Transaction,
    TransactionBuilder,
};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{future, ready, TryStreamExt};
use parking_lot::Mutex;
use postgres_protocol2::message::{backend::Message, frontend};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

use std::time::Duration;

pub struct Responses {
    receiver: mpsc::Receiver<BackendMessages>,
    cur: BackendMessages,
}

impl Responses {
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Message, Error>> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some(Message::ErrorResponse(body)) => return Poll::Ready(Err(Error::db(body))),
                Some(message) => return Poll::Ready(Ok(message)),
                None => {}
            }

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

pub struct InnerClient {
    sender: mpsc::UnboundedSender<Request>,

    /// A buffer to use when writing out postgres commands.
    buffer: Mutex<BytesMut>,
}

impl InnerClient {
    pub fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let (sender, receiver) = mpsc::channel(1);
        let request = Request { messages, sender };
        self.sender.send(request).map_err(|_| Error::closed())?;

        Ok(Responses {
            receiver,
            cur: BackendMessages::empty(),
        })
    }

    /// Call the given function with a buffer to be used when writing out
    /// postgres commands.
    pub fn with_buf<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let mut buffer = self.buffer.lock();
        let r = f(&mut buffer);
        buffer.clear();
        r
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SocketConfig {
    pub host: Host,
    pub port: u16,
    pub connect_timeout: Option<Duration>,
    // pub keepalive: Option<KeepaliveConfig>,
}

/// An asynchronous PostgreSQL client.
///
/// The client is one half of what is returned when a connection is established. Users interact with the database
/// through this client object.
pub struct Client {
    inner: Arc<InnerClient>,

    socket_config: SocketConfig,
    ssl_mode: SslMode,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Request>,
        socket_config: SocketConfig,
        ssl_mode: SslMode,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            inner: Arc::new(InnerClient {
                sender,
                buffer: Default::default(),
            }),

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

    pub(crate) fn inner(&self) -> &Arc<InnerClient> {
        &self.inner
    }

    /// Pass text directly to the Postgres backend to allow it to sort out typing itself and
    /// to save a roundtrip
    pub async fn query_raw_txt<S, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = Option<S>>,
        I::IntoIter: ExactSizeIterator,
    {
        query::query_txt(&self.inner, statement, params).await
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
    pub async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_raw(query).await?.try_collect().await
    }

    pub(crate) async fn simple_query_raw(&self, query: &str) -> Result<SimpleQueryStream, Error> {
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
    pub async fn batch_execute(&self, query: &str) -> Result<ReadyForQueryStatus, Error> {
        simple_query::batch_execute(self.inner(), query).await
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        struct RollbackIfNotDone<'me> {
            client: &'me Client,
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
                let _ = self
                    .client
                    .inner()
                    .send(RequestMessages::Single(FrontendMessage::Raw(buf)));
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
            self.batch_execute("BEGIN").await?;
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
