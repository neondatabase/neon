use pq_proto::CancelKeyData;
use redis::AsyncCommands;
use uuid::Uuid;

use super::notifications::{CancelSession, Notification, PROXY_CHANNEL_NAME};

pub struct RedisPublisherClient {
    client: redis::Client,
    publisher: Option<redis::aio::Connection>,
    region_id: String,
}

impl RedisPublisherClient {
    pub fn new(url: &str, region_id: String) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self {
            client,
            publisher: None,
            region_id,
        })
    }
    pub async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        match self.publish(cancel_key_data, session_id).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to publish a message: {e}");
                self.publisher = None;
            }
        }
        tracing::info!("Publisher is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.publish(cancel_key_data, session_id).await
    }

    async fn publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        let conn = self
            .publisher
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let payload = serde_json::to_string(&Notification::Cancel(CancelSession {
            region_id: Some(self.region_id.clone()),
            cancel_key_data,
            session_id: session_id.into(),
        }))?;
        conn.publish(PROXY_CHANNEL_NAME, payload).await?;
        Ok(())
    }
    pub async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.get_async_connection().await {
            Ok(conn) => {
                self.publisher = Some(conn);
            }
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e.into());
            }
        }
        Ok(())
    }
}
