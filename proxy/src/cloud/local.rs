//! Local mock of Cloud API V2.

use super::api::{self, Api, AuthInfo, DatabaseInfo};
use crate::auth::ClientCredentials;
use crate::scram;
use async_trait::async_trait;

/// Mocked cloud for testing purposes.
pub struct Local {
    /// Database url, e.g. `postgres://user:password@localhost:5432/database`.
    pub url: reqwest::Url,
}

#[async_trait]
impl Api for Local {
    async fn get_auth_info(
        &self,
        creds: &ClientCredentials,
    ) -> Result<AuthInfo, api::GetAuthInfoError> {
        // We wrap `tokio_postgres::Error` because we don't want to infect the
        // method's error type with a detail that's specific to debug mode only.
        let io_error = |e| std::io::Error::new(std::io::ErrorKind::Other, e);

        // Perhaps we could persist this connection, but then we'd have to
        // write more code for reopening it if it got closed, which doesn't
        // seem worth it.
        let (client, connection) =
            tokio_postgres::connect(self.url.as_str(), tokio_postgres::NoTls)
                .await
                .map_err(io_error)?;

        tokio::spawn(connection);
        let query = "select rolpassword from pg_catalog.pg_authid where rolname = $1";
        let rows = client
            .query(query, &[&creds.user])
            .await
            .map_err(io_error)?;

        match &rows[..] {
            // We can't get a secret if there's no such user.
            [] => Err(api::GetAuthInfoError::BadCredentials(creds.to_owned())),
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
                    .ok_or(api::GetAuthInfoError::BadSecret)
            }
        }
    }

    async fn wake_compute(
        &self,
        creds: &ClientCredentials,
    ) -> Result<DatabaseInfo, api::WakeComputeError> {
        // Local setup doesn't have a dedicated compute node,
        // so we just return the local database we're pointed at.
        Ok(DatabaseInfo {
            host: self.url.host_str().unwrap_or("localhost").to_owned(),
            port: self.url.port().unwrap_or(5432),
            dbname: creds.dbname.to_owned(),
            user: creds.user.to_owned(),
            password: None,
        })
    }
}
