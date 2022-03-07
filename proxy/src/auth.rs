use crate::compute::DatabaseInfo;
use crate::config::ProxyConfig;
use crate::cplane_api::{self, CPlaneApi};
use crate::stream::PqStream;
use anyhow::{anyhow, bail, Context};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use zenith_utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage, FeMessage as Fe};

// TODO rename the struct to ClientParams or something
/// Various client credentials which we use for authentication.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,
    pub options: Option<String>,
}

impl TryFrom<HashMap<String, String>> for ClientCredentials {
    type Error = anyhow::Error;

    fn try_from(mut value: HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut get_param = |key| {
            value
                .remove(key)
                .with_context(|| format!("{} is missing in startup packet", key))
        };

        let user = get_param("user")?;
        let dbname = get_param("database")?;

        // TODO see what other options should be recognized, possibly all.
        let options = match get_param("search_path") {
            Ok(path) => Some(format!("-c search_path={}", path)),
            Err(_) => None,
        };

        // TODO investigate why "" is always a key
        // TODO warn on unrecognized options?

        Ok(Self {
            user,
            dbname,
            options,
        })
    }
}

impl ClientCredentials {
    /// Use credentials to authenticate the user.
    pub async fn authenticate(
        self,
        config: &ProxyConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    ) -> anyhow::Result<DatabaseInfo> {
        use crate::config::ClientAuthMethod::*;
        use crate::config::RouterConfig::*;
        let db_info = match &config.router_config {
            Static { host, port } => handle_static(host.clone(), *port, client, self).await,
            Dynamic(Mixed) => {
                if self.user.ends_with("@zenith") {
                    handle_existing_user(config, client, self).await
                } else {
                    handle_new_user(config, client).await
                }
            }
            Dynamic(Password) => handle_existing_user(config, client, self).await,
            Dynamic(Link) => handle_new_user(config, client).await,
        };

        db_info.context("failed to authenticate client")
    }
}

fn new_psql_session_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

async fn handle_static(
    host: String,
    port: u16,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: ClientCredentials,
) -> anyhow::Result<DatabaseInfo> {
    client
        .write_message(&Be::AuthenticationCleartextPassword)
        .await?;

    // Read client's password bytes
    let msg = match client.read_message().await? {
        Fe::PasswordMessage(msg) => msg,
        bad => bail!("unexpected message type: {:?}", bad),
    };

    let cleartext_password = std::str::from_utf8(&msg)?.split('\0').next().unwrap();

    let db_info = DatabaseInfo {
        host,
        port,
        dbname: creds.dbname.clone(),
        user: creds.user.clone(),
        password: Some(cleartext_password.into()),
        options: creds.options,
    };

    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&BeParameterStatusMessage::encoding())?;

    Ok(db_info)
}

async fn handle_existing_user(
    config: &ProxyConfig,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: ClientCredentials,
) -> anyhow::Result<DatabaseInfo> {
    let psql_session_id = new_psql_session_id();
    let md5_salt = rand::random();

    client
        .write_message(&Be::AuthenticationMD5Password(&md5_salt))
        .await?;

    // Read client's password hash
    let msg = match client.read_message().await? {
        Fe::PasswordMessage(msg) => msg,
        bad => bail!("unexpected message type: {:?}", bad),
    };

    let (_trailing_null, md5_response) = msg
        .split_last()
        .ok_or_else(|| anyhow!("unexpected password message"))?;

    let cplane = CPlaneApi::new(&config.auth_endpoint);
    let db_info_response = cplane
        .authenticate_proxy_request(&creds, md5_response, &md5_salt, &psql_session_id)
        .await?;

    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&BeParameterStatusMessage::encoding())?;

    Ok(DatabaseInfo {
        host: db_info_response.host,
        port: db_info_response.port,
        dbname: db_info_response.dbname,
        user: db_info_response.user,
        password: db_info_response.password,
        options: creds.options,
    })
}

async fn handle_new_user(
    config: &ProxyConfig,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> anyhow::Result<DatabaseInfo> {
    let psql_session_id = new_psql_session_id();
    let greeting = hello_message(&config.redirect_uri, &psql_session_id);

    let db_info_response = cplane_api::with_waiter(psql_session_id, |waiter| async {
        // Give user a URL to spawn a new database
        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&Be::NoticeResponse(greeting))
            .await?;

        // Wait for web console response
        waiter.await?.map_err(|e| anyhow!(e))
    })
    .await?;

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database.".into()))?;

    Ok(DatabaseInfo {
        host: db_info_response.host,
        port: db_info_response.port,
        dbname: db_info_response.dbname,
        user: db_info_response.user,
        password: db_info_response.password,
        options: None,
    })
}

fn hello_message(redirect_uri: &str, session_id: &str) -> String {
    format!(
        concat![
            "☀️  Welcome to Zenith!\n",
            "To proceed with database creation, open the following link:\n\n",
            "    {redirect_uri}{session_id}\n\n",
            "It needs to be done once and we will send you '.pgpass' file,\n",
            "which will allow you to access or create ",
            "databases without opening your web browser."
        ],
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
}
