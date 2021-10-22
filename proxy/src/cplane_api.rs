use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};

#[derive(Serialize, Deserialize, Debug)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProxyAuthResult {
    pub ready: bool,
    pub error: Option<String>,
    pub conn_info: Option<DatabaseInfo>,
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

pub struct CPlaneApi {
    auth_endpoint: &'static str,
}

impl CPlaneApi {
    pub fn new(auth_endpoint: &'static str) -> CPlaneApi {
        CPlaneApi { auth_endpoint }
    }

    pub fn authenticate_proxy_request(
        &self,
        user: &str,
        database: &str,
        md5_response: &[u8],
        salt: &[u8; 4],
        psql_session_id: &str,
    ) -> anyhow::Result<ProxyAuthResult> {
        let mut url = reqwest::Url::parse(self.auth_endpoint)?;
        url.query_pairs_mut()
            .append_pair("login", user)
            .append_pair("database", database)
            .append_pair("md5response", std::str::from_utf8(md5_response)?)
            .append_pair("salt", &hex::encode(salt))
            .append_pair("psql_session_id", psql_session_id);

        println!("cplane request: {}", url.as_str());

        let resp = reqwest::blocking::get(url)?;

        if !resp.status().is_success() {
            bail!("Auth failed: {}", resp.status())
        }

        let auth_info: ProxyAuthResult = serde_json::from_str(resp.text()?.as_str())?;
        println!("got auth info: #{:?}", auth_info);
        Ok(auth_info)
    }
}
