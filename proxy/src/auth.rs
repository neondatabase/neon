use crate::cplane_api::DatabaseInfo;
use pin_project_lite::pin_project;
use crate::stream::PqStream;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::state::ProxyWaiters;
use zenith_utils::pq_proto::{BeMessage as Be, *};
use async_trait::async_trait;


// TODO this should be a trait instead, but async_trait is a bit finicky.
#[non_exhaustive]
pub enum Auth {
    Forward(ForwardAuth),
    Md5(Md5Auth),
    Link(LinkAuth),
}

pub async fn authenticate(
    auth: Auth,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: &crate::cplane_api::ClientCredentials,
) -> anyhow::Result<DatabaseInfo> {
    match auth {
        Auth::Forward(auth) => auth.authenticate(client, creds).await,
        Auth::Md5(auth) => auth.authenticate(client, creds).await,
        Auth::Link(auth) => auth.authenticate(client, creds).await,
    }
}

/// Read cleartext password, use it to auth.
/// NOTE Don't use in production.
pub struct ForwardAuth {
    pub host: String,
    pub port: u16,
}

/// Use password-based auth in [`AuthStream`].
pub struct Md5Auth {
    pub auth_endpoint: String,
    pub waiters: ProxyWaiters,
    pub session_id: String,
}

/// Login via link to console
pub struct LinkAuth {
    pub redirect_uri: String,
    pub session_id: String,
    pub waiters: ProxyWaiters,
}

fn hello_message(redirect_uri: &str, session_id: &str) -> String {
    format!(
        concat![
            "☀️  Welcome to Zenith!\n",
            "To proceed with database creation, open the following link:\n\n",
            "    {redirect_uri}{session_id}\n\n",
            "It needs to be done once and we will send you '.pgpass' file,\n",
            "which will allow you to access or create ",
            "databases without opening your web browser."
        ],
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
}

// #[async_trait(?Send)]
impl LinkAuth {
    pub async fn authenticate(
        &self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        _creds: &crate::cplane_api::ClientCredentials,
    ) -> anyhow::Result<DatabaseInfo> {
        let greeting = hello_message(&self.redirect_uri, &self.session_id);

        // First, register this session
        // TODO is there need for this?
        let waiter = self.waiters.register(self.session_id.clone());

        // Give user a URL to spawn a new database
        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&Be::NoticeResponse(greeting)).await?;

        // Wait for web console response
        // TODO is this blocking?
        let db_info = waiter.wait()?.map_err(|e| anyhow::anyhow!(e))?;

        client.write_message(&Be::NoticeResponse("Connecting to database.".into())).await?;

        Ok(db_info)
    }
}

// #[async_trait(?Send)]
impl Md5Auth {
    pub async fn authenticate(
        &self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        creds: &crate::cplane_api::ClientCredentials,
    ) -> anyhow::Result<DatabaseInfo> {
        let md5_salt = rand::random::<[u8; 4]>();

        // Ask password
        client.write_message(&Be::AuthenticationMD5Password(&md5_salt)).await?;

        // Check password
        let msg = client.read_password_message().await?;

        let (_trailing_null, md5_response) = msg
            .split_last()
            .ok_or_else(|| anyhow::anyhow!("unexpected password message"))?;

        let cplane = crate::cplane_api::CPlaneApi::new(&self.auth_endpoint, &self.waiters);
        let db_info = cplane.authenticate_proxy_request(
            &creds.user,
            &creds.dbname,
            md5_response,
            &md5_salt,
            &self.session_id,
        )?;

        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?;

        Ok(db_info)
    }
}

impl ForwardAuth {
    pub async fn authenticate(
        &self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        creds: &crate::cplane_api::ClientCredentials,
    ) -> anyhow::Result<DatabaseInfo> {
        client.write_message(&Be::AuthenticationCleartextPassword).await?;
        let cleartext_password_bytes = client.read_password_message().await?;
        let cleartext_password = std::str::from_utf8(&cleartext_password_bytes)?
            .split('\0').next().unwrap();

        let db_info = crate::cplane_api::DatabaseInfo {
            host: self.host.clone(),
            port: self.port,
            dbname: creds.dbname.clone(),
            user: creds.user.clone(),
            password: Some(cleartext_password.into()),
        };

        Ok(db_info)
    }
}
