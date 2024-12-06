use crate::query::{self, RowStream};
use crate::types::Type;
use crate::{Client, Error, Transaction};
use async_trait::async_trait;
use postgres_protocol2::Oid;

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
#[async_trait]
pub trait GenericClient: private::Sealed {
    async fn query_raw_txt<S, I>(&mut self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send;

    /// Query for type information
    async fn get_type(&mut self, oid: Oid) -> Result<Type, Error>;
}

impl private::Sealed for Client {}

#[async_trait]
impl GenericClient for Client {
    async fn query_raw_txt<S, I>(&mut self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send,
    {
        query::query_txt(&mut self.inner, statement, params).await
    }

    /// Query for type information
    async fn get_type(&mut self, oid: Oid) -> Result<Type, Error> {
        crate::prepare::get_type(&mut self.inner, &mut self.cached_typeinfo, oid).await
    }
}

impl private::Sealed for Transaction<'_> {}

#[async_trait]
#[allow(clippy::needless_lifetimes)]
impl GenericClient for Transaction<'_> {
    async fn query_raw_txt<S, I>(&mut self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send,
    {
        query::query_txt(&mut self.client().inner, statement, params).await
    }

    /// Query for type information
    async fn get_type(&mut self, oid: Oid) -> Result<Type, Error> {
        let client = self.client();
        crate::prepare::get_type(&mut client.inner, &mut client.cached_typeinfo, oid).await
    }
}
