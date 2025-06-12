use std::{
    collections::{HashMap, hash_map},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use futures::{StreamExt, stream::FuturesUnordered};
use safekeeper_api::models::{
    TenantShardPageserverAttachment, TenantShardPageserverAttachmentChange,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, error, info, info_span};
use utils::{
    generation::Generation,
    id::{NodeId, TenantId},
    logging::SecretString,
    shard::ShardIndex,
};

use crate::{
    heartbeater::SafekeeperState,
    persistence::{Persistence, SkPsDiscoveryPersistence},
};

use super::Service;

struct Actor {
    service: Arc<Service>,
    persistence: Arc<Persistence>,
    http_client: reqwest::Client,
}

pub async fn run(service: Arc<Service>, http_client: reqwest::Client) {
    let actor = Actor {
        persistence: service.persistence.clone(),
        service,
        http_client, // XXX: build our own client instead of getting Service's client; we probably want idle conn to each sk
    };
    actor.run().await;
}

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

        let mut sync_full_ticker = tokio::time::interval(std::time::Duration::from_secs(5));

        struct Task {
            work: SkPsDiscoveryPersistence,
            cancel: CancellationToken,
            join_handle: Option<JoinHandle<()>>,
        }
        let mut tasks = HashMap::new();

        loop {
            tokio::select! {
                biased; // control messages have higher priority, the periodic full tick, then subscriptions.
                _ = sync_full_ticker.tick() => {
                    info!("rebuild");
                }
                maybe_res = subscription.next() => {
                    match maybe_res {
                        None => {
                            anyhow::bail!("subscription should never end");
                        }
                        Some(Ok(tenant_id)) => {
                            let tenant_id: TenantId = tenant_id;
                            info!(?tenant_id, "notify for tenant_id");
                            // for now, just also rebuild everything
                        }
                        Some(Err(err)) => {
                            let err: serde_json::Error = err;
                            anyhow::bail!("incorrect notification format: {err:?}"); // FIXME repeat message in error so it can be debugged ?
                        }
                    }
                }
            }

            // get list of tasks from database
            let mut new_tasks = self
                .persistence
                .get_all_sk_ps_discovery_work()
                .await
                .context("get_all_sk_ps_discovery_work")?
                .into_iter()
                .map(|work: SkPsDiscoveryPersistence| {
                    (
                        work.primary_key(),
                        Task {
                            work,
                            cancel: CancellationToken::new(),
                            join_handle: None,
                        },
                    )
                })
                .collect::<HashMap<_, _>>();

            // Carry over ongoing tasks
            let mut cancelled_wait = FuturesUnordered::new();
            for (
                task_key,
                Task {
                    work: ongoing_persistence,
                    cancel,
                    join_handle,
                },
            ) in tasks.drain()
            {
                match new_tasks.entry(task_key) {
                    hash_map::Entry::Occupied(mut planned) => {
                        let Task {
                            work: planned_persistence,
                            cancel: planned_cancel,
                            join_handle: planned_jh,
                        } = planned.get_mut();
                        assert!(planned_jh.is_none());
                        if *planned_persistence == ongoing_persistence {
                            *planned_jh = join_handle;
                            *planned_cancel = cancel;
                            continue;
                        }
                    }
                    hash_map::Entry::Vacant(_) => (),
                }
                cancel.cancel();
                cancelled_wait.push(async move {
                    if let Some(jh) = join_handle {
                        let _ = jh.await;
                    }
                });
            }
            while let Some(_) = cancelled_wait.next().await {}
            tasks = new_tasks;

            // Kick off new tasks
            for (key, task) in tasks.iter_mut() {
                if task.join_handle.is_none() {
                    task.join_handle = Some(tokio::spawn(
                        DeliveryAttempt {
                            cancel: task.cancel.clone(),
                            persistence: self.persistence.clone(),
                            service: self.service.clone(),
                            http_client: self.http_client.clone(),
                            work: task.work.clone(),
                        }
                        .run()
                        .instrument({
                            let span = info_span!(parent: None, "sk_ps_discovery_delivery", ?key);
                            span.follows_from(Span::current());
                            span
                        }),
                    ))
                }
            }
        }
    }
}

struct DeliveryAttempt {
    cancel: CancellationToken,
    persistence: Arc<Persistence>,
    service: Arc<super::Service>,
    http_client: reqwest::Client,
    work: SkPsDiscoveryPersistence,
}

impl DeliveryAttempt {
    pub async fn run(self) {
        let res = self.run0().await;
        if self.cancel.is_cancelled() {
            return;
        }
        if let Err(ref err) = res {
            error!(?err, "attempt failed");
        }
        let res = self
            .persistence
            .update_sk_ps_discovery_attempt(
                self.work.primary_key(),
                self.work.intent_state.clone(),
                res.map_err(|_| ()),
            )
            .await;
        if let Err(ref err) = res {
            error!(?err, "persistence of attempt result failed");
        }
    }
    async fn run0(&self) -> anyhow::Result<()> {
        let Some(sk) = self.service.get_safekeeper_object(self.work.sk_id) else {
            anyhow::bail!("safekeeper object does not exist");
        };

        match sk.availability() {
            SafekeeperState::Available { .. } => (),
            SafekeeperState::Offline => {
                anyhow::bail!("safekeeper is offline");
            }
        }

        let body = {
            let val = TenantShardPageserverAttachment {
                shard_id: ShardIndex {
                    shard_number: utils::shard::ShardNumber(self.work.shard_number as u8),
                    shard_count: utils::shard::ShardCount(self.work.shard_count as u8),
                },
                ps_id: NodeId(self.work.ps_id as u64),
                generation: Generation::new(self.work.ps_generation as u32),
            };
            match self.work.intent_state.as_str() {
                "attached" => TenantShardPageserverAttachmentChange::Attach { field1: val },
                "detached" => TenantShardPageserverAttachmentChange::Detach(val),
                x => anyhow::bail!("unknown intent state {x:?}"),
            }
        };
        let tenant_shard_id = self.work.tenant_shard_id()?;
        sk.with_client_retries(
            |client| {
                let body = body.clone();
                async move {
                    client
                        .post_tenant_shard_pageserver_attachments(tenant_shard_id, body)
                        .await
                }
            },
            &self.http_client,
            &self
                .service
                .config
                .safekeeper_jwt_token
                .clone()
                .map(SecretString::from),
            1,
            3,
            Duration::from_secs(1),
            &self.cancel,
        )
        .await?;

        Ok(())
    }
}
