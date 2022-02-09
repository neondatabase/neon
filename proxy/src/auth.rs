use crate::db::AuthSecret;
use crate::stream::PqStream;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use zenith_utils::pq_proto::BeMessage as Be;


/// Stored secret for authenticating the user via md5 but authenticating
/// to the compute database with a (possibly different) plaintext password.
pub struct PlaintextStoredSecret {
    pub salt: [u8; 4],
    pub hashed_salted_password: Bytes,
    pub compute_db_password: String,
}

/// Sufficient information to auth user and create AuthSecret
#[non_exhaustive]
pub enum StoredSecret {
    PlaintextPassword(PlaintextStoredSecret),
    // TODO add md5 option?
    // TODO add SCRAM option
}

pub async fn authenticate(
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    stored_secret: StoredSecret
) -> anyhow::Result<AuthSecret> {
    match stored_secret {
        StoredSecret::PlaintextPassword(stored) => {
            client.write_message(&Be::AuthenticationMD5Password(&stored.salt)).await?;
            let provided = client.read_password_message().await?;
            anyhow::ensure!(provided == stored.hashed_salted_password);
            Ok(AuthSecret::Password(stored.compute_db_password))
        },
    }
}

#[async_trait::async_trait]
pub trait SecretStore {
    async fn get_stored_secret(&self, creds: &crate::cplane_api::ClientCredentials) -> anyhow::Result<StoredSecret>;
}
