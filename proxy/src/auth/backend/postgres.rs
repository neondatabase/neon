//! Local mock of Cloud API V2.

use super::{
    console::{self, AuthInfo, GetAuthInfoError, TransportError, WakeComputeError},
    AuthSuccess,
};
use crate::{
    auth::{self, ClientCredentials},
    compute,
    error::io_error,
    scram,
    stream::PqStream,
    url::ApiUrl,
};
use tokio::io::{AsyncRead, AsyncWrite};

#[must_use]
pub(super) struct Api<'a> {
    endpoint: &'a ApiUrl,
    creds: &'a ClientCredentials<'a>,
}

// Helps eliminate graceless `.map_err` calls without introducing another ctor.
impl From<tokio_postgres::Error> for TransportError {
    fn from(e: tokio_postgres::Error) -> Self {
        io_error(e).into()
    }
}

impl<'a> Api<'a> {
    /// Construct an API object containing the auth parameters.
    pub(super) fn new(endpoint: &'a ApiUrl, creds: &'a ClientCredentials) -> Self {
        Self { endpoint, creds }
    }

    /// Authenticate the existing user or throw an error.
    pub(super) async fn handle_user(
        self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<AuthSuccess<compute::ConnCfg>> {
        // We reuse user handling logic from a production module.
        console::handle_user(client, &self, Self::get_auth_info, Self::wake_compute).await
    }

    /// This implementation fetches the auth info from a local postgres instance.
    async fn get_auth_info(&self) -> Result<AuthInfo, GetAuthInfoError> {
        // Perhaps we could persist this connection, but then we'd have to
        // write more code for reopening it if it got closed, which doesn't
        // seem worth it.
        let (client, connection) =
            tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

        tokio::spawn(connection);
        let query = "select rolpassword from pg_catalog.pg_authid where rolname = $1";
        let rows = client.query(query, &[&self.creds.user]).await?;

        match &rows[..] {
            // We can't get a secret if there's no such user.
            [] => Err(io_error(format!("unknown user '{}'", self.creds.user)).into()),

            // We shouldn't get more than one row anyway.
            [row, ..] => {
                let entry = row
                    .try_get("rolpassword")
                    .map_err(|e| io_error(format!("failed to read user's password: {e}")))?;

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
                    .ok_or(GetAuthInfoError::BadSecret)
            }
        }
    }

    /// We don't need to wake anything locally, so we just return the connection info.
    pub(super) async fn wake_compute(&self) -> Result<compute::ConnCfg, WakeComputeError> {
        let mut config = compute::ConnCfg::new();
        config
            .host(self.endpoint.host_str().unwrap_or("localhost"))
            .port(self.endpoint.port().unwrap_or(5432))
            .dbname(self.creds.dbname)
            .user(self.creds.user);

        Ok(config)
    }
}
