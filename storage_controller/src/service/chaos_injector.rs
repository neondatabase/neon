use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use pageserver_api::controller_api::ShardSchedulingPolicy;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio_util::sync::CancellationToken;
use utils::id::NodeId;
use utils::shard::TenantShardId;

use super::{Node, Scheduler, Service, TenantShard};

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

    /// If a shard has a secondary and attached location, then re-assign the secondary to be
    /// attached and the attached to be secondary.
    ///
    /// Only modifies tenants if they're in Active scheduling policy.
    fn maybe_migrate_to_secondary(
        &self,
        tenant_shard_id: TenantShardId,
        nodes: &Arc<HashMap<NodeId, Node>>,
        tenants: &mut BTreeMap<TenantShardId, TenantShard>,
        scheduler: &mut Scheduler,
    ) {
        let shard = tenants
            .get_mut(&tenant_shard_id)
            .expect("Held lock between choosing ID and this get");

        if !matches!(shard.get_scheduling_policy(), ShardSchedulingPolicy::Active) {
            // Skip non-active scheduling policies, so that a shard with a policy like Pause can
            // be pinned without being disrupted by us.
            tracing::info!(
                "Skipping shard {tenant_shard_id}: scheduling policy is {:?}",
                shard.get_scheduling_policy()
            );
            return;
        }

        // Pick a secondary to promote
        let Some(new_location) = shard
            .intent
            .get_secondary()
            .choose(&mut thread_rng())
            .cloned()
        else {
            tracing::info!(
                "Skipping shard {tenant_shard_id}: no secondary location, can't migrate"
            );
            return;
        };

        let Some(old_location) = *shard.intent.get_attached() else {
            tracing::info!("Skipping shard {tenant_shard_id}: currently has no attached location");
            return;
        };

        tracing::info!("Injecting chaos: migrate {tenant_shard_id} {old_location}->{new_location}");

        shard.intent.demote_attached(scheduler, old_location);
        shard.intent.promote_attached(scheduler, new_location);
        self.service.maybe_reconcile_shard(shard, nodes);
    }

    async fn inject_chaos(&mut self) {
        // Pick some shards to interfere with
        let batch_size = 128;
        let mut inner = self.service.inner.write().unwrap();
        let (nodes, tenants, scheduler) = inner.parts_mut();
        let tenant_ids = tenants.keys().cloned().collect::<Vec<_>>();

        // Prefer to migrate tenants that are currently outside their home AZ.  This avoids the chaos injector
        // continuously pushing tenants outside their home AZ: instead, we'll tend to cycle between picking some
        // random tenants to move, and then on next chaos iteration moving them back, then picking some new
        // random tenants on the next iteration.
        let mut victims = Vec::with_capacity(batch_size);
        for shard in tenants.values() {
            if shard.is_attached_outside_preferred_az(nodes) {
                victims.push(shard.tenant_shard_id);
            }

            if victims.len() >= batch_size {
                break;
            }
        }

        let choose_random = batch_size.saturating_sub(victims.len());
        tracing::info!("Injecting chaos: found {} shards to migrate back to home AZ, picking {choose_random} random shards to migrate", victims.len());

        let random_victims = tenant_ids.choose_multiple(&mut thread_rng(), choose_random);
        victims.extend(random_victims);

        for victim in victims {
            self.maybe_migrate_to_secondary(victim, nodes, tenants, scheduler);
        }
    }
}
