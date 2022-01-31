///
/// Utils for connecting with the postgres dataabase.
///

use crate::scram::key::ScramKey;
use std::net::{SocketAddr, ToSocketAddrs};
use anyhow::{Context, anyhow};

/// Sufficient information to authenticate as client.
pub struct ScramAuthSecret {
    pub iterations: u32,
    pub salt_base64: String,
    pub client_key: ScramKey,
    pub server_key: ScramKey,
}

#[non_exhaustive]
pub enum AuthSecret {
    Scram(ScramAuthSecret),
    Password(String),
}


pub struct DatabaseAuthInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub auth_secret: AuthSecret,
}

impl From<DatabaseAuthInfo> for tokio_postgres::Config {
    fn from(auth_info: DatabaseAuthInfo) -> Self {
        let mut config = tokio_postgres::Config::new();

        config
            .host(&auth_info.host)
            .port(auth_info.port)
            .dbname(&auth_info.dbname)
            .user(&auth_info.user);

        match auth_info.auth_secret {
            AuthSecret::Scram(scram_secret) => {
                config.add_scram_key(
                    scram_secret.salt_base64.into_bytes(),  // TODO test this
                    scram_secret.iterations,
                    scram_secret.client_key.bytes.to_vec(),
                    scram_secret.server_key.bytes.to_vec(),
                );
            },
            AuthSecret::Password(password) => {
                config.password(password);
            }
        }

        config
    }
}

impl DatabaseAuthInfo {
    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        let host_port = format!("{}:{}", self.host, self.port);
        host_port
            .to_socket_addrs()
            .with_context(|| format!("cannot resolve {} to SocketAddr", host_port))?
            .next()
            .ok_or_else(|| anyhow!("cannot resolve at least one SocketAddr"))
    }
}
