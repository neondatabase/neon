use bytes::Bytes;

use crate::{auth::{PlaintextStoredSecret, SecretStore, StoredSecret}, compute::ComputeProvider, cplane_api::ClientCredentials, db::DatabaseConnInfo};


pub struct MockConsole {
}

#[async_trait::async_trait]
impl SecretStore for MockConsole {
    async fn get_stored_secret(&self, creds: &ClientCredentials) -> anyhow::Result<StoredSecret> {
        let salt = [0; 4];
        match (&creds.user[..], &creds.dbname[..]) {
            ("postgres", "postgres") => Ok(StoredSecret::PlaintextPassword(PlaintextStoredSecret {
                salt,
                hashed_salted_password: "md52fff09cd9def51601fc5445943b3a11f\0".into(),
                compute_db_password: "postgres".into(),
            })),
            _ => unimplemented!()
        }
    }
}

#[async_trait::async_trait]
impl ComputeProvider for MockConsole{
    async fn get_compute_node(&self, creds: &ClientCredentials) -> anyhow::Result<DatabaseConnInfo> {
        return Ok(DatabaseConnInfo {
            host: "127.0.0.1".into(),
            port: 5432,
        })
    }
}
