use crate::console::caches::{ApiCaches, AuthInfoCacheKey};
use futures::StreamExt;
use serde::Deserialize;

const CHANNEL_NAME: &str = "proxy_notifications";

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Notification<'a> {
    UserChangedPassword { project: &'a str, role: &'a str },
}

/// Handle console's invalidation messages.
#[tracing::instrument(name = "console_notifications", skip_all)]
pub async fn task_main(url: String, caches: &ApiCaches) -> anyhow::Result<()> {
    let client = redis::Client::open(url.as_ref())?;
    let mut conn = client.get_async_connection().await?.into_pubsub();

    conn.subscribe(CHANNEL_NAME).await?;
    let mut stream = conn.on_message();

    while let Some(msg) = stream.next().await {
        let payload: String = msg.get_payload()?;

        use Notification::*;
        match serde_json::from_str(&payload) {
            Ok(UserChangedPassword { project, role }) => {
                caches.auth_info.remove(&AuthInfoCacheKey {
                    project: project.into(),
                    role: role.into(),
                });
            }
            Err(e) => tracing::error!("broken message: {e}"),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {}
