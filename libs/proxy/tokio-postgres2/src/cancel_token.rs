use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::client::SocketConfig;
use crate::config::SslMode;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Error, cancel_query, cancel_query_raw};

/// A cancellation token that allows easy cancellation of a query.
#[derive(Clone)]
pub struct CancelToken {
    pub socket_config: SocketConfig,
    pub raw: RawCancelToken,
}

/// A raw cancellation token that allows cancellation of a query, given a fresh connection to postgres.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawCancelToken {
    pub ssl_mode: SslMode,
    pub process_id: i32,
    pub secret_key: i32,
}

impl CancelToken {
    /// Attempts to cancel the in-progress query on the connection associated
    /// with this `CancelToken`.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Cancellation is inherently racy. There is no guarantee that the
    /// cancellation request will reach the server before the query terminates
    /// normally, or that the connection associated with this token is still
    /// active.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    pub async fn cancel_query<T>(&self, tls: T) -> Result<(), Error>
    where
        T: MakeTlsConnect<TcpStream>,
    {
        cancel_query::cancel_query(
            self.socket_config.clone(),
            self.raw.ssl_mode,
            tls,
            self.raw.process_id,
            self.raw.secret_key,
        )
        .await
    }
}

impl RawCancelToken {
    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub async fn cancel_query_raw<S, T>(&self, stream: S, tls: T) -> Result<(), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        cancel_query_raw::cancel_query_raw(
            stream,
            self.ssl_mode,
            tls,
            self.process_id,
            self.secret_key,
        )
        .await
    }
}
