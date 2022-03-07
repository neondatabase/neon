use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};

/// Compute node connection params.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
    pub options: Option<String>,
}

impl DatabaseInfo {
    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        let host_port = format!("{}:{}", self.host, self.port);
        host_port
            .to_socket_addrs()
            .with_context(|| format!("cannot resolve {} to SocketAddr", host_port))?
            .next()
            .context("cannot resolve at least one SocketAddr")
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

        if let Some(options) = db_info.options {
            config.options(&options);
        }

        if let Some(password) = db_info.password {
            config.password(password);
        }

        config
    }
}
