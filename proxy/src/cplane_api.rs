use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};

use crate::state::ProxyWaiters;

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ProxyAuthResponse {
    Ready { conn_info: DatabaseInfo },
    Error { error: String },
    NotReady { ready: bool }, // TODO: get rid of `ready`
}

impl DatabaseInfo {
    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        let host_port = format!("{}:{}", self.host, self.port);
        host_port
            .to_socket_addrs()
            .with_context(|| format!("cannot resolve {} to SocketAddr", host_port))?
            .next()
            .ok_or_else(|| anyhow!("cannot resolve at least one SocketAddr"))
    }
}

impl From<DatabaseInfo> for tokio_postgres::Config {
    fn from(db_info: DatabaseInfo) -> Self {
        let mut config = tokio_postgres::Config::new();

        config
            .host(&db_info.host)
            .port(db_info.port)
            .dbname(&db_info.dbname)
            .user(&db_info.user);

        if let Some(password) = db_info.password {
            config.password(password);
        }

        config
    }
}

pub struct CPlaneApi<'a> {
    auth_endpoint: &'a str,
    waiters: &'a ProxyWaiters,
}

impl<'a> CPlaneApi<'a> {
    pub fn new(auth_endpoint: &'a str, waiters: &'a ProxyWaiters) -> Self {
        Self {
            auth_endpoint,
            waiters,
        }
    }
}

impl CPlaneApi<'_> {
    pub fn authenticate_proxy_request(
        &self,
        user: &str,
        database: &str,
        md5_response: &[u8],
        salt: &[u8; 4],
        psql_session_id: &str,
    ) -> anyhow::Result<DatabaseInfo> {
        let mut url = reqwest::Url::parse(self.auth_endpoint)?;
        url.query_pairs_mut()
            .append_pair("login", user)
            .append_pair("database", database)
            .append_pair("md5response", std::str::from_utf8(md5_response)?)
            .append_pair("salt", &hex::encode(salt))
            .append_pair("psql_session_id", psql_session_id);

        let waiter = self.waiters.register(psql_session_id.to_owned());

        println!("cplane request: {}", url);
        let resp = reqwest::blocking::get(url)?;
        if !resp.status().is_success() {
            bail!("Auth failed: {}", resp.status())
        }

        let auth_info: ProxyAuthResponse = serde_json::from_str(resp.text()?.as_str())?;
        println!("got auth info: #{:?}", auth_info);

        use ProxyAuthResponse::*;
        match auth_info {
            Ready { conn_info } => Ok(conn_info),
            Error { error } => bail!(error),
            NotReady { .. } => waiter.wait()?.map_err(|e| anyhow!(e)),
        }
    }
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
