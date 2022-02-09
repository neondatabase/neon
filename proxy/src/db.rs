///
/// Utils for connecting with the postgres dataabase.
///

use std::net::{SocketAddr, ToSocketAddrs};
use anyhow::{Context, anyhow};

use crate::cplane_api::ClientCredentials;

pub struct DatabaseConnInfo {
    pub host: String,
    pub port: u16,
}

pub struct DatabaseAuthInfo {
    pub conn_info: DatabaseConnInfo,
    pub creds: ClientCredentials,
    pub auth_secret: AuthSecret,
}

/// Sufficient information to auth with database
#[non_exhaustive]
#[derive(Debug)]
pub enum AuthSecret {
    Password(String),
    // TODO add SCRAM option
}

impl From<DatabaseAuthInfo> for tokio_postgres::Config {
    fn from(auth_info: DatabaseAuthInfo) -> Self {
        let mut config = tokio_postgres::Config::new();

        config
            .host(&auth_info.conn_info.host)
            .port(auth_info.conn_info.port)
            .dbname(&auth_info.creds.dbname)
            .user(&auth_info.creds.user);

        match auth_info.auth_secret {
            AuthSecret::Password(password) => {
                config.password(password);
            }
        }

        config
    }
}

impl DatabaseConnInfo {
    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        let host_port = format!("{}:{}", self.host, self.port);
        host_port
            .to_socket_addrs()
            .with_context(|| format!("cannot resolve {} to SocketAddr", host_port))?
            .next()
            .ok_or_else(|| anyhow!("cannot resolve at least one SocketAddr"))
    }
}
