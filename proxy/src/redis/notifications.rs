use std::{convert::Infallible, sync::Arc};

use futures::StreamExt;
use redis::aio::PubSub;
use serde::Deserialize;
use smol_str::SmolStr;

use crate::cache::project_info::ProjectInfoCache;

const CHANNEL_NAME: &str = "neondb-proxy-ws-updates";
const RECONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const INVALIDATION_LAG: std::time::Duration = std::time::Duration::from_secs(20);

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "topic", content = "data")]
enum Notification {
    #[serde(
        rename = "/allowed_ips_updated",
        deserialize_with = "deserialize_json_string"
    )]
    AllowedIpsUpdate {
        allowed_ips_update: AllowedIpsUpdate,
    },
    #[serde(
        rename = "/password_updated",
        deserialize_with = "deserialize_json_string"
    )]
    PasswordUpdate { password_update: PasswordUpdate },
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct AllowedIpsUpdate {
    project_id: SmolStr,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct PasswordUpdate {
    project_id: SmolStr,
    role_name: SmolStr,
}
fn deserialize_json_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: for<'de2> serde::Deserialize<'de2>,
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(<D::Error as serde::de::Error>::custom)
}

fn invalidate_cache<C: ProjectInfoCache>(cache: Arc<C>, msg: Notification) {
    use Notification::*;
    match msg {
        AllowedIpsUpdate { allowed_ips_update } => {
            cache.invalidate_allowed_ips_for_project(&allowed_ips_update.project_id)
        }
        PasswordUpdate { password_update } => cache.invalidate_role_secret_for_project(
            &password_update.project_id,
            &password_update.role_name,
        ),
    }
}

#[tracing::instrument(skip(cache))]
fn handle_message<C>(msg: redis::Msg, cache: Arc<C>) -> anyhow::Result<()>
where
    C: ProjectInfoCache + Send + Sync + 'static,
{
    let payload: String = msg.get_payload()?;
    tracing::debug!(?payload, "received a message payload");

    let msg: Notification = match serde_json::from_str(&payload) {
        Ok(msg) => msg,
        Err(e) => {
            tracing::error!("broken message: {e}");
            return Ok(());
        }
    };
    tracing::debug!(?msg, "received a message");
    invalidate_cache(cache.clone(), msg.clone());
    // It might happen that the invalid entry is on the way to be cached.
    // To make sure that the entry is invalidated, let's repeat the invalidation in INVALIDATION_LAG seconds.
    // TODO: include the version (or the timestamp) in the message and invalidate only if the entry is cached before the message.
    tokio::spawn(async move {
        tokio::time::sleep(INVALIDATION_LAG).await;
        invalidate_cache(cache, msg.clone());
    });

    Ok(())
}

/// Handle console's invalidation messages.
#[tracing::instrument(name = "console_notifications", skip_all)]
pub async fn task_main<C>(url: String, cache: Arc<C>) -> anyhow::Result<Infallible>
where
    C: ProjectInfoCache + Send + Sync + 'static,
{
    cache.enable_ttl();

    loop {
        let redis = ConsoleRedisClient::new(&url)?;
        let conn = match redis.try_connect().await {
            Ok(conn) => {
                cache.disable_ttl();
                conn
            }
            Err(e) => {
                tracing::error!(
                    "failed to connect to redis: {e}, will try to reconnect in {RECONNECT_TIMEOUT:#?}"
                );
                tokio::time::sleep(RECONNECT_TIMEOUT).await;
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
    fn parse_allowed_ips() -> anyhow::Result<()> {
        let project_id = "new_project".to_string();
        let data = format!("{{\"project_id\": \"{project_id}\"}}");
        let text = json!({
            "type": "message",
            "topic": "/allowed_ips_updated",
            "data": data,
            "extre_fields": "something"
        })
        .to_string();

        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(
            result,
            Notification::AllowedIpsUpdate {
                allowed_ips_update: AllowedIpsUpdate {
                    project_id: project_id.into()
                }
            }
        );

        Ok(())
    }

    #[test]
    fn parse_password_updated() -> anyhow::Result<()> {
        let project_id = "new_project".to_string();
        let role_name = "new_role".to_string();
        let data = format!("{{\"project_id\": \"{project_id}\", \"role_name\": \"{role_name}\"}}");
        let text = json!({
            "type": "message",
            "topic": "/password_updated",
            "data": data,
            "extre_fields": "something"
        })
        .to_string();

        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(
            result,
            Notification::PasswordUpdate {
                password_update: PasswordUpdate {
                    project_id: project_id.into(),
                    role_name: role_name.into()
                }
            }
        );

        Ok(())
    }
}
