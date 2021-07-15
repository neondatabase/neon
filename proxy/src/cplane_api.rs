use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
};

pub struct CPlaneApi {
    // address: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub host: IpAddr, // TODO: allow host name here too
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
}

impl DatabaseInfo {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }

    pub fn conn_string(&self) -> String {
        format!(
            "dbname={} user={} password={}",
            self.dbname, self.user, self.password
        )
    }
}

// mock cplane api
impl CPlaneApi {
    pub fn new(_address: &SocketAddr) -> CPlaneApi {
        CPlaneApi {
            // address: address.clone(),
        }
    }

    pub fn check_auth(&self, user: &str, md5_response: &[u8], salt: &[u8; 4]) -> Result<()> {
        // passwords for both is "mypass"
        let auth_map: HashMap<_, &str> = vec![
            ("stas@zenith", "716ee6e1c4a9364d66285452c47402b1"),
            ("stas2@zenith", "3996f75df64c16a8bfaf01301b61d582"),
        ]
        .into_iter()
        .collect();

        let stored_hash = auth_map
            .get(&user)
            .ok_or_else(|| anyhow::Error::msg("user not found"))?;
        let salted_stored_hash = format!(
            "md5{:x}",
            md5::compute([stored_hash.as_bytes(), salt].concat())
        );

        let received_hash = std::str::from_utf8(&md5_response)?;

        println!(
            "auth: {} rh={} sh={} ssh={} {:?}",
            user, received_hash, stored_hash, salted_stored_hash, salt
        );

        if received_hash == salted_stored_hash {
            Ok(())
        } else {
            bail!("Auth failed")
        }
    }

    pub fn get_database_uri(&self, _user: &str, _database: &str) -> Result<DatabaseInfo> {
        Ok(DatabaseInfo {
            host: "127.0.0.1".parse()?,
            port: 5432,
            dbname: "stas".to_string(),
            user: "stas".to_string(),
            password: "mypass".to_string(),
        })
    }

    // pub fn create_database(&self, _user: &String, _database: &String) -> Result<DatabaseInfo> {
    //     Ok(DatabaseInfo {
    //         host: "127.0.0.1".parse()?,
    //         port: 5432,
    //         dbname: "stas".to_string(),
    //         user: "stas".to_string(),
    //         password: "mypass".to_string(),
    //     })
    // }
}
