use crate::auth::ClientCredentials;
use crate::compute::DatabaseInfo;
use crate::error::UserFacingError;
use crate::mgmt;
use crate::waiters::{self, Waiter, Waiters};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use thiserror::Error;

lazy_static! {
    static ref CPLANE_WAITERS: Waiters<mgmt::ComputeReady> = Default::default();
}

/// Give caller an opportunity to wait for cplane's reply.
pub async fn with_waiter<R, T, E>(
    psql_session_id: impl Into<String>,
    action: impl FnOnce(Waiter<'static, mgmt::ComputeReady>) -> R,
) -> Result<T, E>
where
    R: std::future::Future<Output = Result<T, E>>,
    E: From<waiters::RegisterError>,
{
    let waiter = CPLANE_WAITERS.register(psql_session_id.into())?;
    action(waiter).await
}

pub fn notify(
    psql_session_id: &str,
    msg: Result<DatabaseInfo, String>,
) -> Result<(), waiters::NotifyError> {
    CPLANE_WAITERS.notify(psql_session_id, msg)
}

/// Zenith console API wrapper.
pub struct CPlaneApi {
    auth_endpoint: reqwest::Url,
}

impl CPlaneApi {
    pub fn new(auth_endpoint: reqwest::Url) -> Self {
        Self { auth_endpoint }
    }
}

#[derive(Debug, Error)]
pub enum AuthErrorImpl {
    /// Authentication error reported by the console.
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    /// HTTP status (other than 200) returned by the console.
    #[error("Console responded with an HTTP status: {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error("Console responded with a malformed JSON: {0}")]
    MalformedResponse(#[from] serde_json::Error),

    #[error(transparent)]
    Transport(#[from] reqwest::Error),

    #[error(transparent)]
    WaiterRegister(#[from] waiters::RegisterError),

    #[error(transparent)]
    WaiterWait(#[from] waiters::WaitError),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct AuthError(Box<AuthErrorImpl>);

impl AuthError {
    /// Smart constructor for authentication error reported by `mgmt`.
    pub fn auth_failed(msg: impl Into<String>) -> Self {
        AuthError(Box::new(AuthErrorImpl::AuthFailed(msg.into())))
    }
}

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
            AuthFailed(_) | HttpStatus(_) => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}

impl CPlaneApi {
    pub async fn authenticate_proxy_client(
        &self,
        creds: ClientCredentials,
        md5_response: &str,
        salt: &[u8; 4],
        psql_session_id: &str,
    ) -> Result<DatabaseInfo, AuthError> {
        let mut url = self.auth_endpoint.clone();
        url.query_pairs_mut()
            .append_pair("login", &creds.user)
            .append_pair("database", &creds.dbname)
            .append_pair("md5response", md5_response)
            .append_pair("salt", &hex::encode(salt))
            .append_pair("psql_session_id", psql_session_id);

        with_waiter(psql_session_id, |waiter| async {
            println!("cplane request: {}", url);
            // TODO: leverage `reqwest::Client` to reuse connections
            let resp = reqwest::get(url).await?;
            if !resp.status().is_success() {
                return Err(AuthErrorImpl::HttpStatus(resp.status()).into());
            }

            let auth_info: ProxyAuthResponse = serde_json::from_str(resp.text().await?.as_str())?;
            println!("got auth info [redacted]"); // Content contains password, don't print it

            use ProxyAuthResponse::*;
            let db_info = match auth_info {
                Ready { conn_info } => conn_info,
                Error { error } => return Err(AuthErrorImpl::AuthFailed(error).into()),
                NotReady { .. } => waiter.await?.map_err(AuthErrorImpl::AuthFailed)?,
            };

            Ok(db_info)
        })
        .await
    }
}

// NOTE: the order of constructors is important.
// https://serde.rs/enum-representations.html#untagged
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ProxyAuthResponse {
    Ready { conn_info: DatabaseInfo },
    Error { error: String },
    NotReady { ready: bool }, // TODO: get rid of `ready`
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_proxy_auth_response() {
        // Ready
        let auth: ProxyAuthResponse = serde_json::from_value(json!({
            "ready": true,
            "conn_info": DatabaseInfo::default(),
        }))
        .unwrap();
        assert!(matches!(
            auth,
            ProxyAuthResponse::Ready {
                conn_info: DatabaseInfo { .. }
            }
        ));

        // Error
        let auth: ProxyAuthResponse = serde_json::from_value(json!({
            "ready": false,
            "error": "too bad, so sad",
        }))
        .unwrap();
        assert!(matches!(auth, ProxyAuthResponse::Error { .. }));

        // NotReady
        let auth: ProxyAuthResponse = serde_json::from_value(json!({
            "ready": false,
        }))
        .unwrap();
        assert!(matches!(auth, ProxyAuthResponse::NotReady { .. }));
    }
}
