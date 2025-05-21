#![allow(async_fn_in_trait)]

use crate::query::RowStream;
use crate::{Client, Error, Transaction};

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
pub trait GenericClient: private::Sealed {
    /// Like `Client::query_raw_txt`.
    async fn query_raw_txt<S, I>(&mut self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send;
}

impl private::Sealed for Client {}

impl GenericClient for Client {
    async fn query_raw_txt<S, I>(&mut self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send,
    {
        self.query_raw_txt(statement, params).await
    }
}

impl private::Sealed for Transaction<'_> {}

impl GenericClient for Transaction<'_> {
    async fn query_raw_txt<S, I>(&mut self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        S: AsRef<str> + Sync + Send,
        I: IntoIterator<Item = Option<S>> + Sync + Send,
        I::IntoIter: ExactSizeIterator + Sync + Send,
    {
        self.query_raw_txt(statement, params).await
    }
}
