mod credentials;
mod flow;

use crate::config::{CloudApi, ProxyConfig};
use crate::error::UserFacingError;
use crate::stream::PqStream;
use crate::{cloud, compute, waiters};
use std::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage};

pub use credentials::ClientCredentials;
pub use flow::*;

/// Common authentication error.
#[derive(Debug, Error)]
pub enum AuthErrorImpl {
    /// Authentication error reported by the console.
    #[error(transparent)]
    Console(#[from] cloud::AuthError),

    #[error(transparent)]
    GetAuthInfo(#[from] cloud::api::GetAuthInfoError),

    #[error(transparent)]
    WakeCompute(#[from] cloud::api::WakeComputeError),

    #[error(transparent)]
    Sasl(#[from] crate::sasl::Error),

    /// For passwords that couldn't be processed by [`parse_password`].
    #[error("Malformed password message")]
    MalformedPassword,

    /// Errors produced by [`PqStream`].
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl AuthErrorImpl {
    pub fn auth_failed(msg: impl Into<String>) -> Self {
        AuthErrorImpl::Console(cloud::AuthError::auth_failed(msg))
    }
}

impl From<waiters::RegisterError> for AuthErrorImpl {
    fn from(e: waiters::RegisterError) -> Self {
        AuthErrorImpl::Console(cloud::AuthError::from(e))
    }
}

impl From<waiters::WaitError> for AuthErrorImpl {
    fn from(e: waiters::WaitError) -> Self {
        AuthErrorImpl::Console(cloud::AuthError::from(e))
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

async fn handle_user(
    config: &ProxyConfig,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: ClientCredentials,
) -> Result<compute::NodeInfo, AuthError> {
    if creds.is_existing_user() {
        match &config.cloud_endpoint {
            CloudApi::V1(api) => handle_existing_user_v1(api, client, creds).await,
            CloudApi::V2(api) => handle_existing_user_v2(api.as_ref(), client, creds).await,
        }
    } else {
        let redirect_uri = config.redirect_uri.as_ref();
        handle_new_user(redirect_uri, client).await
    }
}

/// Authenticate user via a legacy cloud API endpoint.
async fn handle_existing_user_v1(
    cloud: &cloud::Legacy,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: ClientCredentials,
) -> Result<compute::NodeInfo, AuthError> {
    let psql_session_id = new_psql_session_id();
    let md5_salt = rand::random();

    client
        .write_message(&Be::AuthenticationMD5Password(md5_salt))
        .await?;

    // Read client's password hash
    let msg = client.read_password_message().await?;
    let md5_response = parse_password(&msg).ok_or(AuthErrorImpl::MalformedPassword)?;

    let db_info = cloud
        .authenticate_proxy_client(creds, md5_response, &md5_salt, &psql_session_id)
        .await?;

    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&BeParameterStatusMessage::encoding())?;

    Ok(compute::NodeInfo {
        db_info,
        scram_keys: None,
    })
}

/// Authenticate user via a new cloud API endpoint which supports SCRAM.
async fn handle_existing_user_v2(
    cloud: &(impl cloud::Api + ?Sized),
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: ClientCredentials,
) -> Result<compute::NodeInfo, AuthError> {
    let auth_info = cloud.get_auth_info(&creds).await?;

    let flow = AuthFlow::new(client);
    let scram_keys = match auth_info {
        cloud::api::AuthInfo::Md5(_) => {
            // TODO: decide if we should support MD5 in api v2
            return Err(AuthErrorImpl::auth_failed("MD5 is not supported").into());
        }
        cloud::api::AuthInfo::Scram(secret) => {
            let scram = Scram(&secret);
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
        db_info: cloud.wake_compute(&creds).await?,
        scram_keys,
    })
}

async fn handle_new_user(
    redirect_uri: &str,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> Result<compute::NodeInfo, AuthError> {
    let psql_session_id = new_psql_session_id();
    let greeting = hello_message(redirect_uri, &psql_session_id);

    let db_info = cloud::with_waiter(psql_session_id, |waiter| async {
        // Give user a URL to spawn a new database
        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&Be::NoticeResponse(&greeting))
            .await?;

        // Wait for web console response (see `mgmt`)
        waiter.await?.map_err(AuthErrorImpl::auth_failed)
    })
    .await?;

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database."))?;

    Ok(compute::NodeInfo {
        db_info,
        scram_keys: None,
    })
}

fn new_psql_session_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

fn parse_password(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()?.strip_suffix('\0')
}

fn hello_message(redirect_uri: &str, session_id: &str) -> String {
    format!(
        concat![
            "☀️  Welcome to Neon!\n",
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
