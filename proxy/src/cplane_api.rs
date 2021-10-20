use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};

pub struct CPlaneApi {
    auth_endpoint: &'static str,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
}

impl DatabaseInfo {
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        let host_port = format!("{}:{}", self.host, self.port);
        host_port
            .to_socket_addrs()
            .with_context(|| format!("cannot resolve {} to SocketAddr", host_port))?
            .next()
            .ok_or_else(|| anyhow::Error::msg("cannot resolve at least one SocketAddr"))
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
    ) -> Result<DatabaseInfo> {
        let mut url = reqwest::Url::parse(self.auth_endpoint)?;
        url.query_pairs_mut()
            .append_pair("login", user)
            .append_pair("database", database)
            .append_pair("md5response", std::str::from_utf8(md5_response)?)
            .append_pair("salt", &hex::encode(salt));

        println!("cplane request: {}", url.as_str());

        let resp = reqwest::blocking::get(url)?;

        if resp.status().is_success() {
            let conn_info: DatabaseInfo = serde_json::from_str(resp.text()?.as_str())?;
            println!("got conn info: #{:?}", conn_info);
            Ok(conn_info)
        } else {
            bail!("Auth failed")
        }
    }
}
