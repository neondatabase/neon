use crate::compute::DatabaseInfo;
use crate::config::ProxyConfig;
use crate::cplane_api::{self, CPlaneApi};
use crate::error::UserFacingError;
use crate::stream::PqStream;
use crate::waiters;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use zenith_utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage};

/// Common authentication error.
#[derive(Debug, Error)]
pub enum AuthErrorImpl {
    /// Authentication error reported by the console.
    #[error(transparent)]
    Console(#[from] cplane_api::AuthError),

    /// For passwords that couldn't be processed by [`parse_password`].
    #[error("Malformed password message")]
    MalformedPassword,

    /// Errors produced by [`PqStream`].
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl AuthErrorImpl {
    pub fn auth_failed(msg: impl Into<String>) -> Self {
        AuthErrorImpl::Console(cplane_api::AuthError::auth_failed(msg))
    }
}

impl From<waiters::RegisterError> for AuthErrorImpl {
    fn from(e: waiters::RegisterError) -> Self {
        AuthErrorImpl::Console(cplane_api::AuthError::from(e))
    }
}

impl From<waiters::WaitError> for AuthErrorImpl {
    fn from(e: waiters::WaitError) -> Self {
        AuthErrorImpl::Console(cplane_api::AuthError::from(e))
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct AuthError(Box<AuthErrorImpl>);

impl<T> From<T> for AuthError
where
    AuthErrorImpl: From<T>,
{
    fn from(e: T) -> Self {
        AuthError(Box::new(e.into()))
    }
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        use AuthErrorImpl::*;
        match self.0.as_ref() {
            Console(e) => e.to_string_client(),
            MalformedPassword => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientCredsParseError {
    #[error("Parameter `{0}` is missing in startup packet")]
    MissingKey(&'static str),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
#[derive(Debug, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,
}

impl TryFrom<HashMap<String, String>> for ClientCredentials {
    type Error = ClientCredsParseError;

    fn try_from(mut value: HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut get_param = |key| {
            value
                .remove(key)
                .ok_or(ClientCredsParseError::MissingKey(key))
        };

        let user = get_param("user")?;
        let db = get_param("database")?;

        Ok(Self { user, dbname: db })
    }
}

impl ClientCredentials {
    /// Use credentials to authenticate the user.
    pub async fn authenticate(
        self,
        config: &ProxyConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    ) -> Result<DatabaseInfo, AuthError> {
        fail::fail_point!("proxy-authenticate", |_| {
            Err(AuthError::auth_failed("failpoint triggered"))
        });

        use crate::config::ClientAuthMethod::*;
        use crate::config::RouterConfig::*;
        match &config.router_config {
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
        }
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
) -> Result<DatabaseInfo, AuthError> {
    client
        .write_message(&Be::AuthenticationCleartextPassword)
        .await?;

    // Read client's password bytes
    let msg = client.read_password_message().await?;
    let cleartext_password = parse_password(&msg).ok_or(AuthErrorImpl::MalformedPassword)?;

    let db_info = DatabaseInfo {
        host,
        port,
        dbname: creds.dbname.clone(),
        user: creds.user.clone(),
        password: Some(cleartext_password.into()),
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
) -> Result<DatabaseInfo, AuthError> {
    let psql_session_id = new_psql_session_id();
    let md5_salt = rand::random();

    client
        .write_message(&Be::AuthenticationMD5Password(&md5_salt))
        .await?;

    // Read client's password hash
    let msg = client.read_password_message().await?;
    let md5_response = parse_password(&msg).ok_or(AuthErrorImpl::MalformedPassword)?;

    let cplane = CPlaneApi::new(config.auth_endpoint.clone());
    let db_info = cplane
        .authenticate_proxy_client(creds, md5_response, &md5_salt, &psql_session_id)
        .await?;

    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&BeParameterStatusMessage::encoding())?;

    Ok(db_info)
}

async fn handle_new_user(
    config: &ProxyConfig,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> Result<DatabaseInfo, AuthError> {
    let psql_session_id = new_psql_session_id();
    let greeting = hello_message(&config.redirect_uri, &psql_session_id);

    let db_info = cplane_api::with_waiter(psql_session_id, |waiter| async {
        // Give user a URL to spawn a new database
        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&Be::NoticeResponse(greeting))
            .await?;

        // Wait for web console response (see `mgmt`)
        waiter.await?.map_err(AuthErrorImpl::auth_failed)
    })
    .await?;

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database.".into()))?;

    Ok(db_info)
}

fn parse_password(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()?.strip_suffix('\0')
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
