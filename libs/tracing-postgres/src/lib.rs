//!
//! Wrapper around postgres::Client that adds tracing spans to executed queries,
//! commits, etc.
//!

use std::str::FromStr;
use tracing::instrument;

// For convenience of callers, re-export various submodules, structs,
// etc from the 'postgres' so that callers don't need to use anything
// from the postgres crate directly. This way, you don't need 'postgres'
// module directly at all. It is a nice way to enforce that all your
// calls go through the wrappers.
//
// A few things are missing from here, because they aren't currently
// needed by any of the code that uses this wrapper. If you need something
// that's missing, feel free to add it.
//
// These are listed in the same order as in the docs at
// https://docs.rs/postgres/latest/postgres/index.html, so that it's
// easy to compare and if anything is missing.

// Re-exports
pub use postgres::{
    // Config is implemented as a wrapper
    error::Error,
    row::Row,
    row::SimpleQueryRow,
    tls::NoTls,
};

// Modules
pub use postgres::{binary_copy, error, notifications, row, tls, types};

// Structs
pub use postgres::{
    CancelToken,
    // Client is implemented as a wrapper
    Column,
    CopyInWriter,
    CopyOutReader,
    GenericClient,
    Notification,
    Portal,
    RowIter,
    Socket,
    Statement,
    // Transaction is implemented as a wrapper
    // TransactionBuilder is currently not implemented
};

// Enums
pub use postgres::{IsolationLevel, SimpleQueryMessage};

// Traits
pub use postgres::ToStatement;

// Wrapped versions of postgres::{Client, Config, Transaction}
pub type Client = TracingClient;
pub type Transaction<'a> = TracingTransaction<'a>;
pub type Config = TracingConfig;

/// Like postgres::Config, but with a `connect` function that returns the wrapped
/// TracingClient
pub struct TracingConfig(postgres::Config);

/// This allows calling all the get_* functions in postgres::Config:
///
/// XXX: Unfortunately, this also allows you to call the un-wrapped connect
/// function, with `config.deref().connect()`, and bypass the tracing. That's
/// not easy to do by accident, though, so we accept it.
impl std::ops::Deref for TracingConfig {
    type Target = postgres::Config;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Allows calling the setter functions in postgres::Config, to set the config.
impl std::ops::DerefMut for TracingConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TracingConfig {
    /// Like postgres::Config::connect, with tracing
    #[instrument(skip_all, fields(host=?self.get_hosts(), ports=?self.get_ports(), dbname=?self.get_dbname()))]
    pub fn connect<T>(self, tls_mode: T) -> Result<TracingClient, Error>
    where
        T: tls::MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as tls::TlsConnect<Socket>>::Future: Send,
    {
        Ok(TracingClient {
            inner: self.0.connect(tls_mode)?,
        })
    }
}

impl TracingClient {
    /// Like postgres::Client::connect, with tracing
    #[instrument(skip(tls_mode))]
    pub fn connect<T>(params: &str, tls_mode: T) -> Result<TracingClient, Error>
    where
        T: tls::MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as tls::TlsConnect<Socket>>::Future: Send,
    {
        Ok(TracingClient {
            inner: postgres::Client::connect(params, tls_mode)?,
        })
    }
}

impl FromStr for TracingConfig {
    type Err = postgres::Error;

    fn from_str(s: &str) -> Result<TracingConfig, Error> {
        Ok(TracingConfig(postgres::Config::from_str(s)?))
    }
}

/// Wrapper that works like Postgres::Client, but adds tracing spans to each call.
pub struct TracingGenericClient<C: GenericClient> {
    inner: C,
}

pub type TracingClient = TracingGenericClient<postgres::Client>;
pub type TracingTransaction<'a> = TracingGenericClient<postgres::Transaction<'a>>;

/// Make TracingGenericClient look like postgres::GenericClient.
///
/// Unfortunately, we cannot actually implement postgres::GenericClient,
/// because it's marked as sealed. One consequence of that is that if a new
/// function is added to postgres::GenericClient, our code will still compile,
/// but you cannot use the new function until you add it here.
impl<C: GenericClient> TracingGenericClient<C> {
    #[instrument(skip_all)]
    pub fn execute<T>(
        &mut self,
        query: &T,
        params: &[&(dyn types::ToSql + Sync)],
    ) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + std::fmt::Debug,
    {
        self.inner.execute(query, params)
    }

    #[instrument(skip_all)]
    pub fn query<T>(
        &mut self,
        query: &T,
        params: &[&(dyn types::ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.inner.query(query, params)
    }

    #[instrument(skip_all)]
    pub fn query_one<T>(
        &mut self,
        query: &T,
        params: &[&(dyn types::ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.inner.query_one(query, params)
    }

    #[instrument(skip_all)]
    fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn types::ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.inner.query_opt(query, params)
    }

    #[instrument(skip_all)]
    pub fn query_raw<T, P, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        P: types::BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.inner.query_raw(query, params)
    }

    #[instrument(skip_all)]
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.inner.prepare(query)
    }

    #[instrument(skip_all)]
    pub fn prepare_typed(
        &mut self,
        query: &str,
        types: &[types::Type],
    ) -> Result<Statement, Error> {
        self.inner.prepare_typed(query, types)
    }

    #[instrument(skip_all)]
    pub fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.inner.copy_in(query)
    }

    #[instrument(skip_all)]
    pub fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.inner.copy_out(query)
    }

    #[instrument(skip(self))]
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.inner.simple_query(query)
    }

    #[instrument(skip(self))]
    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.inner.batch_execute(query)
    }

    #[instrument(skip(self))]
    pub fn transaction(&mut self) -> Result<TracingTransaction<'_>, Error> {
        Ok(TracingTransaction {
            inner: self.inner.transaction()?,
        })
    }
}

/// A few extra functions in postgres::Client that are not part of the
/// GenericClient interface.
///
/// TODO: This doesn't include all the functions in postgres::Client. If
/// you need one that's missing, feel free to add it.
impl TracingClient {
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

/// A few extra functions in postgres::Transaction that are not part of the
/// GenericClient interface.
impl<'a> TracingTransaction<'a> {
    #[instrument(skip_all)]
    pub fn commit(self) -> Result<(), Error> {
        self.inner.commit()
    }

    #[instrument(skip_all)]
    pub fn rollback(self) -> Result<(), Error> {
        self.inner.rollback()
    }
}
