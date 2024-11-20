use std::sync::Arc;

use pq_proto::CancelKeyData;
use redis::AsyncCommands;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use super::notifications::{CancelSession, Notification, PROXY_CHANNEL_NAME};
use crate::rate_limiter::{GlobalRateLimiter, RateBucketInfo};

pub trait CancellationPublisherMut: Send + Sync + 'static {
    #[allow(async_fn_in_trait)]
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()>;
}

pub trait CancellationPublisher: Send + Sync + 'static {
    #[allow(async_fn_in_trait)]
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()>;
}

impl CancellationPublisher for () {
    async fn try_publish(
        &self,
        _cancel_key_data: CancelKeyData,
        _session_id: Uuid,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<P: CancellationPublisher> CancellationPublisherMut for P {
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        <P as CancellationPublisher>::try_publish(self, cancel_key_data, session_id).await
    }
}

impl<P: CancellationPublisher> CancellationPublisher for Option<P> {
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        if let Some(p) = self {
            p.try_publish(cancel_key_data, session_id).await
        } else {
            Ok(())
        }
    }
}

impl<P: CancellationPublisherMut> CancellationPublisher for Arc<Mutex<P>> {
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        self.lock()
            .await
            .try_publish(cancel_key_data, session_id)
            .await
    }
}

pub struct RedisPublisherClient {
    client: ConnectionWithCredentialsProvider,
    region_id: String,
    limiter: GlobalRateLimiter,
}

impl RedisPublisherClient {
    pub fn new(
        client: ConnectionWithCredentialsProvider,
        region_id: String,
        info: &'static [RateBucketInfo],
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            region_id,
            limiter: GlobalRateLimiter::new(info.into()),
        })
    }

    async fn publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&Notification::Cancel(CancelSession {
            region_id: Some(self.region_id.clone()),
            cancel_key_data,
            session_id,
        }))?;
        let _: () = self.client.publish(PROXY_CHANNEL_NAME, payload).await?;
        Ok(())
    }
    pub(crate) async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.connect().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e);
            }
        }
        Ok(())
    }
    async fn try_publish_internal(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        // TODO: review redundant error duplication logs.
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping cancellation message");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }
        match self.publish(cancel_key_data, session_id).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to publish a message: {e}");
            }
        }
        tracing::info!("Publisher is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.publish(cancel_key_data, session_id).await
    }
}

impl CancellationPublisherMut for RedisPublisherClient {
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        tracing::info!("publishing cancellation key to Redis");
        match self.try_publish_internal(cancel_key_data, session_id).await {
            Ok(()) => {
                tracing::debug!("cancellation key successfuly published to Redis");
                Ok(())
            }
            Err(e) => {
                tracing::error!("failed to publish a message: {e}");
                Err(e)
            }
        }
    }
}
