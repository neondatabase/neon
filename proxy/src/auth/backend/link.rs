use crate::{auth, compute, error::UserFacingError, stream::PqStream, waiters};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage};

#[derive(Debug, Error)]
pub enum LinkAuthError {
    /// Authentication error reported by the console.
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error(transparent)]
    WaiterRegister(#[from] waiters::RegisterError),

    #[error(transparent)]
    WaiterWait(#[from] waiters::WaitError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl UserFacingError for LinkAuthError {
    fn to_string_client(&self) -> String {
        use LinkAuthError::*;
        match self {
            AuthFailed(_) => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}

fn hello_message(redirect_uri: &str, session_id: &str) -> String {
    format!(
        concat![
            "Welcome to Neon!\n",
            "Authenticate by visiting:\n",
            "    {redirect_uri}{session_id}\n\n",
        ],
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
}

pub fn new_psql_session_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

pub async fn handle_user(
    redirect_uri: &reqwest::Url,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<compute::NodeInfo> {
    let psql_session_id = new_psql_session_id();
    let greeting = hello_message(redirect_uri.as_str(), &psql_session_id);

    let db_info = super::with_waiter(psql_session_id, |waiter| async {
        // Give user a URL to spawn a new database
        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&Be::NoticeResponse(&greeting))
            .await?;

        // Wait for web console response (see `mgmt`)
        waiter.await?.map_err(LinkAuthError::AuthFailed)
    })
    .await?;

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database."))?;

    Ok(compute::NodeInfo {
        reported_auth_ok: true,
        config: db_info.into(),
    })
}
