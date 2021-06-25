use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

pub struct CPlaneApi {
    address: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub addr: SocketAddr,
    pub connstr: String,
}

// mock cplane api
impl CPlaneApi {
    pub fn new(address: &SocketAddr) -> CPlaneApi {
        CPlaneApi {
            address: address.clone(),
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

    pub fn get_database_uri(&self, _user: &String, _database: &String) -> Result<DatabaseInfo> {
        Ok(DatabaseInfo {
            addr: "127.0.0.1:5432".parse()?,
            connstr: "user=stas dbname=stas".into(),
        })
    }

    pub fn create_database(&self, _user: &String, _database: &String) -> Result<DatabaseInfo> {
        Ok(DatabaseInfo {
            addr: "127.0.0.1:5432".parse()?,
            connstr: "user=stas dbname=stas".into(),
        })
    }
}
