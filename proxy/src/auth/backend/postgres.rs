//! Local mock of Cloud API V2.

use crate::{
    auth::{
        self,
        backend::console::{self, io_error, AuthInfo, Result},
        ClientCredentials, DatabaseInfo,
    },
    compute, scram,
    stream::PqStream,
    url::ApiUrl,
};
use tokio::io::{AsyncRead, AsyncWrite};

#[must_use]
pub(super) struct Api<'a> {
    endpoint: &'a ApiUrl,
    creds: &'a ClientCredentials,
}

impl<'a> Api<'a> {
    /// Construct an API object containing the auth parameters.
    pub(super) fn new(endpoint: &'a ApiUrl, creds: &'a ClientCredentials) -> Result<Self> {
        Ok(Self { endpoint, creds })
    }

    /// Authenticate the existing user or throw an error.
    pub(super) async fn handle_user(
        self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<compute::NodeInfo> {
        // We reuse user handling logic from a production module.
        console::handle_user(client, &self, Self::get_auth_info, Self::wake_compute).await
    }

    /// This implementation fetches the auth info from a local postgres instance.
    async fn get_auth_info(&self) -> Result<AuthInfo> {
        // Perhaps we could persist this connection, but then we'd have to
        // write more code for reopening it if it got closed, which doesn't
        // seem worth it.
        let (client, connection) =
            tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls)
                .await
                .map_err(io_error)?;

        tokio::spawn(connection);
        let query = "select rolpassword from pg_catalog.pg_authid where rolname = $1";
        let rows = client
            .query(query, &[&self.creds.user])
            .await
            .map_err(io_error)?;

        match &rows[..] {
            // We can't get a secret if there's no such user.
            [] => Err(io_error(format!("unknown user '{}'", self.creds.user)).into()),

            // We shouldn't get more than one row anyway.
            [row, ..] => {
                let entry = row.try_get(0).map_err(io_error)?;
                scram::ServerSecret::parse(entry)
                    .map(AuthInfo::Scram)
                    .or_else(|| {
                        // It could be an md5 hash if it's not a SCRAM secret.
                        let text = entry.strip_prefix("md5")?;
                        Some(AuthInfo::Md5({
                            let mut bytes = [0u8; 16];
                            hex::decode_to_slice(text, &mut bytes).ok()?;
                            bytes
                        }))
                    })
                    // Putting the secret into this message is a security hazard!
                    .ok_or(console::ConsoleAuthError::BadSecret)
            }
        }
    }

    /// We don't need to wake anything locally, so we just return the connection info.
    async fn wake_compute(&self) -> Result<DatabaseInfo> {
        Ok(DatabaseInfo {
            // TODO: handle that near CLI params parsing
            host: self.endpoint.host_str().unwrap_or("localhost").to_owned(),
            port: self.endpoint.port().unwrap_or(5432),
            dbname: self.creds.dbname.to_owned(),
            user: self.creds.user.to_owned(),
            password: None,
        })
    }
}
