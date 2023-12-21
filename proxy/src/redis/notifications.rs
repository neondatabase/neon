use std::{convert::Infallible, sync::Arc};

use futures::StreamExt;
use redis::aio::PubSub;
use serde::Deserialize;

use crate::cache::{project_info::LookupInfo, Cache};

const CHANNEL_NAME: &str = "proxy_notifications";

struct ConsoleRedisClient {
    client: redis::Client,
}

impl ConsoleRedisClient {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self { client })
    }
    async fn try_connect(&self) -> anyhow::Result<PubSub> {
        let mut conn = self.client.get_async_connection().await?.into_pubsub();
        tracing::info!("subscribing to a channel `{CHANNEL_NAME}`");
        conn.subscribe(CHANNEL_NAME).await?;
        Ok(conn)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Notification {
    #[serde(rename = "allowed_ips_updated")]
    AllowedIpsUpdate { project: String },
    #[serde(rename = "password_updated")]
    PasswordUpdate { project: String, role: String },
}

impl From<Notification> for LookupInfo {
    fn from(val: Notification) -> Self {
        use Notification::*;
        match val {
            AllowedIpsUpdate { project } => LookupInfo::new_allowed_ips(project.into()),
            PasswordUpdate { project, role } => {
                LookupInfo::new_role_secret(project.into(), role.into())
            }
        }
    }
}

#[tracing::instrument(skip(cache))]
fn handle_message<C>(msg: redis::Msg, cache: Arc<C>) -> anyhow::Result<()>
where
    C: Cache + Send + Sync + 'static,
    Notification: Into<<C as Cache>::LookupInfo<<C as Cache>::Key>>,
    <C as Cache>::LookupInfo<<C as Cache>::Key>: Send,
{
    let payload: String = msg.get_payload()?;

    let msg: Notification = match serde_json::from_str(&payload) {
        Ok(msg) => msg,
        Err(e) => {
            tracing::error!("broken message: {e}");
            return Ok(());
        }
    };
    let lookup_info = msg.into();
    cache.invalidate(&lookup_info);
    // It might happen that the invalid entry is on the way to be cached.
    // To make sure that the entry is invalidated, let's repeat the invalidation in 20 seconds.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        cache.invalidate(&lookup_info);
    });

    Ok(())
}

/// Handle console's invalidation messages.
#[tracing::instrument(name = "console_notifications", skip_all)]
pub async fn task_main<C>(url: String, cache: Arc<C>) -> anyhow::Result<Infallible>
where
    C: Cache + Send + Sync + 'static,
    Notification: Into<<C as Cache>::LookupInfo<<C as Cache>::Key>>,
    <C as Cache>::LookupInfo<<C as Cache>::Key>: Send,
{
    cache.enable_ttl();
    let redis = ConsoleRedisClient::new(&url)?;

    loop {
        let conn = match redis.try_connect().await {
            Ok(conn) => {
                cache.disable_ttl();
                conn
            }
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(100)).await;
                continue;
            }
        };
        let mut stream = conn.into_on_message();
        while let Some(msg) = stream.next().await {
            match handle_message(msg, cache.clone()) {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("failed to handle message: {e}, will try to reconnect");
                    break;
                }
            }
        }
        cache.enable_ttl();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_notification() -> anyhow::Result<()> {
        let text = json!({
            "type": "allowed_ips_updated",
            "project": "new_project",
            "extre_fields": "something"
        })
        .to_string();

        let _: Notification = serde_json::from_str(&text)?;

        Ok(())
    }
}
