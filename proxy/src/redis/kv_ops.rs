use redis::{AsyncCommands, ToRedisArgs};
use std::convert::Infallible;
use tokio::sync::{mpsc, oneshot};

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::rate_limiter::{GlobalRateLimiter, RateBucketInfo};

// Message types for sending through mpsc channel
pub enum RedisOp {
    HSet {
        key: String,
        field: String,
        value: String,
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
    },
    HSetMultiple {
        key: String,
        items: Vec<(String, String)>,
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
    },
    HGet {
        key: String,
        field: String,
        resp_tx: oneshot::Sender<anyhow::Result<String>>,
    },
    HGetAll {
        key: String,
        resp_tx: oneshot::Sender<anyhow::Result<Vec<(String, String)>>>,
    },
    HDel {
        key: String,
        field: String,
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
    },
}

pub struct RedisKVClient {
    client: ConnectionWithCredentialsProvider,
    limiter: GlobalRateLimiter,
    pub rx: mpsc::Receiver<RedisOp>, // recieve messages from other tasks
}

impl RedisKVClient {
    pub fn new(
        client: ConnectionWithCredentialsProvider,
        info: &'static [RateBucketInfo],
        rx: mpsc::Receiver<RedisOp>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            limiter: GlobalRateLimiter::new(info.into()),
            rx,
        })
    }

    pub async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.connect().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e);
            }
        }
        Ok(())
    }

    // Running as a separate task to accept messages through the rx channel
    pub async fn handle_messages(&mut self) -> anyhow::Result<Infallible> {
        loop {
            if let Some(msg) = self.rx.recv().await {
                match msg {
                    RedisOp::HSet {
                        key,
                        field,
                        value,
                        resp_tx,
                    } => {
                        if let Some(resp_tx) = resp_tx {
                            drop(resp_tx.send(self.hset(key, field, value).await));
                        } else {
                            drop(self.hset(key, field, value).await);
                        }
                    }
                    RedisOp::HSetMultiple {
                        key,
                        items,
                        resp_tx,
                    } => {
                        if let Some(resp_tx) = resp_tx {
                            drop(resp_tx.send(self.hset_multiple(&key, &items).await));
                        } else {
                            drop(self.hset_multiple(&key, &items).await);
                        }
                    }
                    RedisOp::HGet {
                        key,
                        field,
                        resp_tx,
                    } => {
                        drop(resp_tx.send(self.hget(key, field).await));
                    }
                    RedisOp::HGetAll { key, resp_tx } => {
                        drop(resp_tx.send(self.hget_all(key).await));
                    }
                    RedisOp::HDel {
                        key,
                        field,
                        resp_tx,
                    } => {
                        if let Some(resp_tx) = resp_tx {
                            drop(resp_tx.send(self.hdel(key, field).await));
                        } else {
                            drop(self.hdel(key, field).await);
                        }
                    }
                }
            }
        }
    }

    async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync + Clone,
        F: ToRedisArgs + Send + Sync + Clone,
        V: ToRedisArgs + Send + Sync + Clone,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self
            .client
            .hset(key.clone(), field.clone(), value.clone())
            .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hset(key, field, value)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn hset_multiple<K, V>(&mut self, key: &str, items: &[(K, V)]) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset_multiple");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hset_multiple(key, items).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hset_multiple(key, items)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn hget<K, F, V>(&mut self, key: K, field: F) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync + Clone,
        F: ToRedisArgs + Send + Sync + Clone,
        V: redis::FromRedisValue,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hget");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hget(key.clone(), field.clone()).await {
            Ok(value) => return Ok(value),
            Err(e) => {
                tracing::error!("failed to get a value: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hget(key, field)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn hget_all<K, V>(&mut self, key: K) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync + Clone,
        V: redis::FromRedisValue,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hgetall");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hgetall(key.clone()).await {
            Ok(value) => return Ok(value),
            Err(e) => {
                tracing::error!("failed to get a value: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client.hgetall(key).await.map_err(anyhow::Error::new)
    }

    async fn hdel<K, F>(&mut self, key: K, field: F) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync + Clone,
        F: ToRedisArgs + Send + Sync + Clone,
    {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hdel");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match self.client.hdel(key.clone(), field.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::error!("failed to delete a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnectiong...");
        self.try_connect().await?;
        self.client
            .hdel(key, field)
            .await
            .map_err(anyhow::Error::new)
    }
}
