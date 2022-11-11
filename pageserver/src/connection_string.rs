use std::fmt::{Debug, Formatter};

/// Contains two versions of a Postgres connection string: one without secrets (can be logged safely),
/// one with secrets (e.g. JWT token) which is only used for connection.
///
/// URLs look much worse in logs because we use urlencoded query parameters extensively,
/// e.g. `?options=-c%20timelineid%3D12345678` instead of `options='-c timelineid=12345678'`.
/// We rarely  "build" connection strings on the fly, so we don't need much structure either.
#[derive(Clone)]
pub struct ConnectionString {
    pub no_secrets: String,
    pub with_secrets: String,
}

impl Debug for ConnectionString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.no_secrets.fmt(f)
    }
}
