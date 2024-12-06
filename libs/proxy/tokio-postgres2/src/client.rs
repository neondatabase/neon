use crate::codec::{BackendMessages, FrontendMessage};

use crate::config::Host;
use crate::config::SslMode;
use crate::connection::{Request, RequestMessages};

use crate::types::{Oid, Type};

use crate::{
    simple_query, CancelToken, Error, ReadyForQueryStatus, Statement, Transaction,
    TransactionBuilder,
};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{future, ready};
use postgres_protocol2::message::{backend::Message, frontend};
use std::collections::HashMap;
use std::fmt;
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

/// A cache of type info and prepared statements for fetching type info
/// (corresponding to the queries in the [prepare] module).
#[derive(Default)]
pub(crate) struct CachedTypeInfo {
    /// A statement for basic information for a type from its
    /// OID. Corresponds to [TYPEINFO_QUERY](prepare::TYPEINFO_QUERY) (or its
    /// fallback).
    typeinfo: Option<Statement>,
    /// A statement for getting information for a composite type from its OID.
    /// Corresponds to [TYPEINFO_QUERY](prepare::TYPEINFO_COMPOSITE_QUERY).
    typeinfo_composite: Option<Statement>,
    /// A statement for getting information for a composite type from its OID.
    /// Corresponds to [TYPEINFO_QUERY](prepare::TYPEINFO_COMPOSITE_QUERY) (or
    /// its fallback).
    typeinfo_enum: Option<Statement>,

    /// Cache of types already looked up.
    types: HashMap<Oid, Type>,
}
impl CachedTypeInfo {
    pub(crate) fn typeinfo(&mut self) -> Option<&Statement> {
        self.typeinfo.as_ref()
    }

    pub(crate) fn set_typeinfo(&mut self, statement: Statement) -> &Statement {
        self.typeinfo.insert(statement)
    }

    pub(crate) fn typeinfo_composite(&mut self) -> Option<&Statement> {
        self.typeinfo_composite.as_ref()
    }

    pub(crate) fn set_typeinfo_composite(&mut self, statement: Statement) -> &Statement {
        self.typeinfo_composite.insert(statement)
    }

    pub(crate) fn typeinfo_enum(&mut self) -> Option<&Statement> {
        self.typeinfo_enum.as_ref()
    }

    pub(crate) fn set_typeinfo_enum(&mut self, statement: Statement) -> &Statement {
        self.typeinfo_enum.insert(statement)
    }

    pub(crate) fn type_(&mut self, oid: Oid) -> Option<Type> {
        self.types.get(&oid).cloned()
    }

    pub(crate) fn set_type(&mut self, oid: Oid, type_: &Type) {
        self.types.insert(oid, type_.clone());
    }
}

pub struct InnerClient {
    sender: mpsc::UnboundedSender<Request>,

    /// A buffer to use when writing out postgres commands.
    buffer: BytesMut,
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
    pub fn with_buf<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let r = f(&mut self.buffer);
        self.buffer.clear();
        r
    }
}

#[derive(Clone)]
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
    pub(crate) inner: InnerClient,
    pub(crate) cached_typeinfo: CachedTypeInfo,

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
            inner: InnerClient {
                sender,
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
        simple_query::batch_execute(&mut self.inner, query).await
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

                let buf = self.client.inner.with_buf(|buf| {
                    frontend::query("ROLLBACK", buf).unwrap();
                    buf.split().freeze()
                });
                let _ = self
                    .client
                    .inner
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
