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
pub struct TransactionBuilder {
    pub isolation_level: Option<IsolationLevel>,
    pub read_only: Option<bool>,
    pub deferrable: Option<bool>,
}

impl TransactionBuilder {
    /// Begins the transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub fn format(self) -> String {
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

        query
    }
}
