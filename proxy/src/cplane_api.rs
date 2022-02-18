use crate::auth::ClientCredentials;
use crate::compute::DatabaseInfo;
use crate::waiters::{Waiter, Waiters};
use anyhow::{anyhow, bail};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref CPLANE_WAITERS: Waiters<Result<DatabaseInfo, String>> = Default::default();
}

/// Give caller an opportunity to wait for cplane's reply.
pub async fn with_waiter<F, R, T>(psql_session_id: impl Into<String>, f: F) -> anyhow::Result<T>
where
    F: FnOnce(Waiter<'static, Result<DatabaseInfo, String>>) -> R,
    R: std::future::Future<Output = anyhow::Result<T>>,
{
    let waiter = CPLANE_WAITERS.register(psql_session_id.into())?;
    f(waiter).await
}

pub fn notify(psql_session_id: &str, msg: Result<DatabaseInfo, String>) -> anyhow::Result<()> {
    CPLANE_WAITERS.notify(psql_session_id, msg)
}

/// Zenith console API wrapper.
pub struct CPlaneApi<'a> {
    auth_endpoint: &'a str,
}

impl<'a> CPlaneApi<'a> {
    pub fn new(auth_endpoint: &'a str) -> Self {
        Self { auth_endpoint }
    }
}

impl CPlaneApi<'_> {
    pub async fn authenticate_proxy_request(
        &self,
        creds: ClientCredentials,
        md5_response: &[u8],
        salt: &[u8; 4],
        psql_session_id: &str,
    ) -> anyhow::Result<DatabaseInfo> {
        let mut url = reqwest::Url::parse(self.auth_endpoint)?;
        url.query_pairs_mut()
            .append_pair("login", &creds.user)
            .append_pair("database", &creds.dbname)
            .append_pair("md5response", std::str::from_utf8(md5_response)?)
            .append_pair("salt", &hex::encode(salt))
            .append_pair("psql_session_id", psql_session_id);

        with_waiter(psql_session_id, |waiter| async {
            println!("cplane request: {}", url);
            // TODO: leverage `reqwest::Client` to reuse connections
            let resp = reqwest::get(url).await?;
            if !resp.status().is_success() {
                bail!("Auth failed: {}", resp.status())
            }

            let auth_info: ProxyAuthResponse = serde_json::from_str(resp.text().await?.as_str())?;
            println!("got auth info [redacted]");  // Content contains password, don't print it

            use ProxyAuthResponse::*;
            match auth_info {
                Ready { conn_info } => Ok(conn_info),
                Error { error } => bail!(error),
                NotReady { .. } => waiter.await?.map_err(|e| anyhow!(e)),
            }
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
