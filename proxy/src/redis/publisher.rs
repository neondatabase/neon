use pq_proto::CancelKeyData;
use redis::AsyncCommands;

use super::notifications::{CancelSession, Notification, PROXY_CHANNEL_NAME};
use crate::context::RequestMonitoring;

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
        ctx: &mut RequestMonitoring,
        cancel_key_data: CancelKeyData,
    ) -> anyhow::Result<()> {
        match self.publish(ctx, cancel_key_data).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to publish a message: {e}");
                self.publisher = None;
            }
        }
        tracing::info!("Publisher is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.publish(ctx, cancel_key_data).await
    }

    async fn publish(
        &mut self,
        ctx: &mut RequestMonitoring,
        cancel_key_data: CancelKeyData,
    ) -> anyhow::Result<()> {
        let conn = self
            .publisher
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let payload = serde_json::to_string(&Notification::Cancel(CancelSession {
            region_id: Some(self.region_id.clone()),
            cancel_key_data,
            session_id: ctx.session_id.into(),
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
