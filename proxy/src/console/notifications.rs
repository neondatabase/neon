use crate::console::caches::{ApiCaches, AuthInfoCacheKey};
use futures::StreamExt;
use serde::Deserialize;

const CHANNEL_NAME: &str = "proxy_notifications";

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Notification<'a> {
    #[serde(rename = "password_changed")]
    PasswordChanged { project: &'a str, role: &'a str },
}

#[tracing::instrument(skip(caches))]
fn handle_message(msg: redis::Msg, caches: &ApiCaches) -> anyhow::Result<()> {
    let payload: String = msg.get_payload()?;

    use Notification::*;
    match serde_json::from_str(&payload) {
        Ok(PasswordChanged { project, role }) => {
            let key = AuthInfoCacheKey {
                project: project.into(),
                role: role.into(),
            };

            tracing::info!(key = ?key, "invalidating auth info");
            caches.auth_info.remove(&key);
        }
        Err(e) => tracing::error!("broken message: {e}"),
    }

    Ok(())
}

/// Handle console's invalidation messages.
#[tracing::instrument(name = "console_notifications", skip_all)]
pub async fn task_main(url: String, caches: &ApiCaches) -> anyhow::Result<()> {
    let client = redis::Client::open(url.as_ref())?;
    let mut conn = client.get_async_connection().await?.into_pubsub();

    tracing::info!("subscribing to a channel `{CHANNEL_NAME}`");
    conn.subscribe(CHANNEL_NAME).await?;
    let mut stream = conn.on_message();

    while let Some(msg) = stream.next().await {
        handle_message(msg, caches)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_notification() -> anyhow::Result<()> {
        let text = json!({
            "type": "password_changed",
            "project": "very-nice",
            "role": "borat",
        })
        .to_string();

        let _: Notification = serde_json::from_str(&text)?;

        Ok(())
    }
}
