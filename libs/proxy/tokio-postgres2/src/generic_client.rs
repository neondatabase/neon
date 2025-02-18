#![allow(async_fn_in_trait)]

use crate::query::RowStream;
use crate::types::Type;
use crate::{Client, Error, Transaction};
use postgres_protocol2::Oid;

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
pub trait GenericClient: private::Sealed {
    /// Like `Client::query_raw_txt`.
    async fn query_raw_txt<S, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send;

    /// Query for type information
    async fn get_type(&self, oid: Oid) -> Result<Type, Error>;
}

impl private::Sealed for Client {}

impl GenericClient for Client {
    async fn query_raw_txt<S, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send,
    {
        self.query_raw_txt(statement, params).await
    }

    /// Query for type information
    async fn get_type(&self, oid: Oid) -> Result<Type, Error> {
        crate::prepare::get_type(self.inner(), oid).await
    }
}

impl private::Sealed for Transaction<'_> {}

impl GenericClient for Transaction<'_> {
    async fn query_raw_txt<S, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send,
    {
        self.query_raw_txt(statement, params).await
    }

    /// Query for type information
    async fn get_type(&self, oid: Oid) -> Result<Type, Error> {
        self.client().get_type(oid).await
    }
}
