use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use tracing::info;
use utils::id::TenantId;

use crate::persistence::Persistence;

pub struct ActorClient {
    tx: tokio::sync::mpsc::UnboundedSender<Message>,
}

struct Actor {
    persistence: Arc<Persistence>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

#[derive(Debug)]
enum Message {}

pub fn spawn(persistence: Arc<Persistence>) -> ActorClient {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let actor = Actor { persistence, rx };
    tokio::spawn(actor.run());
    ActorClient { tx }
}

impl ActorClient {}

impl Actor {
    async fn run(mut self) {
        loop {
            match self.run0().await {
                Ok(()) => {
                    info!("sk_ps_discovery actor exiting after shutdown signal observed");
                    return;
                }
                Err(err) => {
                    tracing::error!(
                        ?err,
                        "sk_ps_discovery actor encountered an error, restarting after backoff"
                    );
                    // TODO: proper backoff
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn run0(&mut self) -> anyhow::Result<()> {
        let mut subscription = self
            .persistence
            .listen_sk_ps_discovery()
            .await
            .context("listen to sk_ps_discovery")?;

        loop {
            tokio::select! {
                maybe_res = subscription.next() => {
                    match maybe_res {
                        None => {
                            anyhow::bail!("subscription should never end");
                        }
                        Some(Ok(tenant_id)) => {
                            let tenant_id: TenantId = tenant_id;
                            info!(?tenant_id, "notify for tenant_id");
                        }
                        Some(Err(err)) => {
                            let err: serde_json::Error = err;
                            anyhow::bail!("incorrect notification format: {err:?}"); // FIXME repeat message in error so it can be debugged ?
                        }
                    }
                }
                msg = self.rx.recv() => {
                    todo!("{msg:?}");
                }
            }
        }
    }
}
