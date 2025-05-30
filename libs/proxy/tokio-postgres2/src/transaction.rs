use crate::query::RowStream;
use crate::{CancelToken, Client, Error, ReadyForQueryStatus};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a mut Client,
    done: bool,
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let _ = self.client.inner_mut().send_simple_query("ROLLBACK");
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Transaction<'a> {
        Transaction {
            client,
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub async fn commit(mut self) -> Result<ReadyForQueryStatus, Error> {
        self.done = true;
        self.client.batch_execute("COMMIT").await
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub async fn rollback(mut self) -> Result<ReadyForQueryStatus, Error> {
        self.done = true;
        self.client.batch_execute("ROLLBACK").await
    }

    /// Like `Client::query_raw_txt`.
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
        self.client.query_raw_txt(statement, params).await
    }

    /// Like `Client::cancel_token`.
    pub fn cancel_token(&self) -> CancelToken {
        self.client.cancel_token()
    }

    /// Returns a reference to the underlying `Client`.
    pub fn client(&self) -> &Client {
        self.client
    }

    /// Returns a reference to the underlying `Client`.
    pub fn client_mut(&mut self) -> &mut Client {
        self.client
    }
}
