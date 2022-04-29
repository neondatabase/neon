//! Local mock of Cloud API V2.

use super::console::{self, AuthInfo, DatabaseInfo};
use crate::scram;
use crate::{auth::ClientCredentials, compute};

use crate::stream::PqStream;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage};

async fn get_auth_info(
    auth_endpoint: &str,
    creds: &ClientCredentials,
) -> Result<AuthInfo, console::ConsoleAuthError> {
    // We wrap `tokio_postgres::Error` because we don't want to infect the
    // method's error type with a detail that's specific to debug mode only.
    let io_error = |e| std::io::Error::new(std::io::ErrorKind::Other, e);

    // Perhaps we could persist this connection, but then we'd have to
    // write more code for reopening it if it got closed, which doesn't
    // seem worth it.
    let (client, connection) = tokio_postgres::connect(auth_endpoint, tokio_postgres::NoTls)
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
        [] => Err(console::ConsoleAuthError::BadCredentials(creds.to_owned())),
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

pub async fn handle_user(
    auth_endpoint: &reqwest::Url,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: &ClientCredentials,
) -> Result<compute::NodeInfo, crate::auth::AuthError> {
    let auth_info = get_auth_info(auth_endpoint.as_ref(), creds).await?;

    let flow = crate::auth::AuthFlow::new(client);
    let scram_keys = match auth_info {
        AuthInfo::Md5(_) => {
            // TODO: decide if we should support MD5 in api v2
            return Err(crate::auth::AuthErrorImpl::auth_failed("MD5 is not supported").into());
        }
        AuthInfo::Scram(secret) => {
            let scram = crate::auth::Scram(&secret);
            Some(compute::ScramKeys {
                client_key: flow.begin(scram).await?.authenticate().await?.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            })
        }
    };

    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&BeParameterStatusMessage::encoding())?;

    Ok(compute::NodeInfo {
        db_info: DatabaseInfo {
            // TODO: handle that near CLI params parsing
            host: auth_endpoint.host_str().unwrap_or("localhost").to_owned(),
            port: auth_endpoint.port().unwrap_or(5432),
            dbname: creds.dbname.to_owned(),
            user: creds.user.to_owned(),
            password: None,
        },
        scram_keys,
    })
}
