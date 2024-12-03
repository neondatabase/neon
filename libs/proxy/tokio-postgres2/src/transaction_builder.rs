use crate::{Client, Error, Transaction};

/// The isolation level of a database transaction.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// Equivalent to `ReadCommitted`.
    ReadUncommitted,

    /// An individual statement in the transaction will see rows committed before it began.
    ReadCommitted,

    /// All statements in the transaction will see the same view of rows committed before the first query in the
    /// transaction.
    RepeatableRead,

    /// The reads and writes in this transaction must be able to be committed as an atomic "unit" with respect to reads
    /// and writes of all other concurrent serializable transactions without interleaving.
    Serializable,
}

/// A builder for database transactions.
pub struct TransactionBuilder<'a> {
    client: &'a mut Client,
    isolation_level: Option<IsolationLevel>,
    read_only: Option<bool>,
    deferrable: Option<bool>,
}

impl<'a> TransactionBuilder<'a> {
    pub(crate) fn new(client: &'a mut Client) -> TransactionBuilder<'a> {
        TransactionBuilder {
            client,
            isolation_level: None,
            read_only: None,
            deferrable: None,
        }
    }

    /// Sets the isolation level of the transaction.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = Some(isolation_level);
        self
    }

    /// Sets the access mode of the transaction.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = Some(read_only);
        self
    }

    /// Sets the deferrability of the transaction.
    ///
    /// If the transaction is also serializable and read only, creation of the transaction may block, but when it
    /// completes the transaction is able to run with less overhead and a guarantee that it will not be aborted due to
    /// serialization failure.
    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.deferrable = Some(deferrable);
        self
    }

    /// Begins the transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub async fn start(self) -> Result<Transaction<'a>, Error> {
        let mut query = "START TRANSACTION".to_string();
        let mut first = true;

        if let Some(level) = self.isolation_level {
            first = false;

            query.push_str(" ISOLATION LEVEL ");
            let level = match level {
                IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
                IsolationLevel::ReadCommitted => "READ COMMITTED",
                IsolationLevel::RepeatableRead => "REPEATABLE READ",
                IsolationLevel::Serializable => "SERIALIZABLE",
            };
            query.push_str(level);
        }

        if let Some(read_only) = self.read_only {
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

        if let Some(deferrable) = self.deferrable {
            if !first {
                query.push(',');
            }

            let s = if deferrable {
                " DEFERRABLE"
            } else {
                " NOT DEFERRABLE"
            };
            query.push_str(s);
        }

        self.client.batch_execute(&query).await?;

        Ok(Transaction::new(self.client))
    }
}
