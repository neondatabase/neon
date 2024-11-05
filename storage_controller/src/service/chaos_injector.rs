use std::{sync::Arc, time::Duration};

use pageserver_api::controller_api::ShardSchedulingPolicy;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio_util::sync::CancellationToken;

use super::Service;

pub struct ChaosInjector {
    service: Arc<Service>,
    interval: Duration,
}

impl ChaosInjector {
    pub fn new(service: Arc<Service>, interval: Duration) -> Self {
        Self { service, interval }
    }

    pub async fn run(&mut self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = cancel.cancelled() => {
                    tracing::info!("Shutting down");
                    return;
                }
            }

            self.inject_chaos().await;

            tracing::info!("Chaos iteration...");
        }
    }

    async fn inject_chaos(&mut self) {
        // Pick some shards to interfere with
        let batch_size = 128;
        let mut inner = self.service.inner.write().unwrap();
        let (nodes, tenants, scheduler) = inner.parts_mut();
        let tenant_ids = tenants.keys().cloned().collect::<Vec<_>>();
        let victims = tenant_ids.choose_multiple(&mut thread_rng(), batch_size);

        for victim in victims {
            let shard = tenants
                .get_mut(victim)
                .expect("Held lock between choosing ID and this get");

            if !matches!(shard.get_scheduling_policy(), ShardSchedulingPolicy::Active) {
                // Skip non-active scheduling policies, so that a shard with a policy like Pause can
                // be pinned without being disrupted by us.
                tracing::info!(
                    "Skipping shard {victim}: scheduling policy is {:?}",
                    shard.get_scheduling_policy()
                );
                continue;
            }

            // Pick a secondary to promote
            let Some(new_location) = shard
                .intent
                .get_secondary()
                .choose(&mut thread_rng())
                .cloned()
            else {
                tracing::info!("Skipping shard {victim}: no secondary location, can't migrate");
                continue;
            };

            let Some(old_location) = *shard.intent.get_attached() else {
                tracing::info!("Skipping shard {victim}: currently has no attached location");
                continue;
            };

            tracing::info!("Injecting chaos: migrate {victim} {old_location}->{new_location}");

            shard.intent.demote_attached(scheduler, old_location);
            shard.intent.promote_attached(scheduler, new_location);
            self.service.maybe_reconcile_shard(shard, nodes);
        }
    }
}
