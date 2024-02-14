use std::hash::BuildHasherDefault;

use lasso::{Spur, ThreadedRodeo};
use pq_proto::CancelKeyData;
use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, FromRedisValue,
};
use rustc_hash::FxHasher;
use uuid::Uuid;

use crate::rate_limiter::{RateBucketInfo, RedisRateLimiter};

use super::notifications::{CancelSession, Notification, PROXY_CHANNEL_NAME};

pub struct RedisPublisherClient {
    client: redis::Client,
    publisher: Option<redis::aio::Connection>,
    region_id: String,
    limiter: RedisRateLimiter,
}

impl RedisPublisherClient {
    pub async fn new(
        url: &str,
        region_id: String,
        info: &'static [RateBucketInfo],
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;

        let mut conn = client.get_async_connection().await.unwrap();

        let len: u64 = conn.xlen("neon:endpoints:testing").await.unwrap();
        tracing::info!("starting with {len} endpoints in redis");

        for i in len..300000u64 {
            let _key: String = conn
                .xadd(
                    "neon:endpoints:testing",
                    "*",
                    &[("endpoint_id", format!("ep-hello-world-{i}"))],
                )
                .await
                .unwrap();
            if i.is_power_of_two() {
                tracing::debug!("endpoints written {i}");
            }
            // client.
        }
        tracing::info!("written endpoints to redis");

        // start reading

        let s = ThreadedRodeo::<Spur, BuildHasherDefault<FxHasher>>::with_hasher(
            BuildHasherDefault::default(),
        );
        let opts = StreamReadOptions::default().count(1000);
        let mut id = "0-0".to_string();
        loop {
            let mut res: StreamReadReply = conn
                .xread_options(&["neon:endpoints:testing"], &[&id], &opts)
                .await
                .unwrap();
            if res.keys.is_empty() {
                break;
            }

            assert_eq!(res.keys.len(), 1);
            let res = res.keys.pop().unwrap();
            assert_eq!(res.key, "neon:endpoints:testing");

            if res.ids.is_empty() {
                break;
            }
            for x in res.ids {
                id = x.id;
                if let Some(ep) = x.map.get("endpoint_id") {
                    let ep = String::from_redis_value(ep).unwrap();
                    s.get_or_intern(ep);

                    if s.len().is_power_of_two() {
                        tracing::debug!("endpoints read {}", s.len());
                    }
                }
            }
        }
        tracing::info!("read {} endpoints from redis", s.len());

        Ok(Self {
            client,
            publisher: Some(conn),
            region_id,
            limiter: RedisRateLimiter::new(info),
        })
    }
    pub async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
    ) -> anyhow::Result<()> {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping cancellation message");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }
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
            session_id,
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
