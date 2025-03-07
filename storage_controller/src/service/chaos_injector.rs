use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

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
    chaos_exit_crontab: Option<cron::Schedule>,
}

fn cron_to_next_duration(cron: &cron::Schedule) -> anyhow::Result<tokio::time::Sleep> {
    use chrono::Utc;
    let next = cron.upcoming(Utc).next().unwrap();
    let duration = (next - Utc::now()).to_std()?;
    Ok(tokio::time::sleep(duration))
}

async fn maybe_sleep(sleep: Option<tokio::time::Sleep>) -> Option<()> {
    if let Some(sleep) = sleep {
        sleep.await;
        Some(())
    } else {
        None
    }
}

impl ChaosInjector {
    pub fn new(
        service: Arc<Service>,
        interval: Duration,
        chaos_exit_crontab: Option<cron::Schedule>,
    ) -> Self {
        Self {
            service,
            interval,
            chaos_exit_crontab,
        }
    }

    fn get_cron_interval_sleep_future(&self) -> Option<tokio::time::Sleep> {
        if let Some(ref chaos_exit_crontab) = self.chaos_exit_crontab {
            match cron_to_next_duration(chaos_exit_crontab) {
                Ok(interval_exit) => Some(interval_exit),
                Err(e) => {
                    tracing::error!("Error processing the cron schedule: {e}");
                    None
                }
            }
        } else {
            None
        }
    }

    pub async fn run(&mut self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(self.interval);
        #[derive(Debug)]
        enum ChaosEvent {
            ShuffleTenant,
            ForceKill,
        }
        loop {
            let cron_interval = self.get_cron_interval_sleep_future();
            let chaos_type = tokio::select! {
                _ = interval.tick() => {
                    ChaosEvent::ShuffleTenant
                }
                Some(_) = maybe_sleep(cron_interval) => {
                    ChaosEvent::ForceKill
                }
                _ = cancel.cancelled() => {
                    tracing::info!("Shutting down");
                    return;
                }
            };
            tracing::info!("Chaos iteration: {chaos_type:?}...");
            match chaos_type {
                ChaosEvent::ShuffleTenant => {
                    self.inject_chaos().await;
                }
                ChaosEvent::ForceKill => {
                    self.force_kill().await;
                }
            }
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
        self.service.maybe_reconcile_shard(
            shard,
            nodes,
            crate::reconciler::ReconcilerPriority::Normal,
        );
    }

    async fn force_kill(&mut self) {
        tracing::warn!("Injecting chaos: force kill");
        std::process::exit(1);
    }

    async fn inject_chaos(&mut self) {
        // Pick some shards to interfere with
        let batch_size = 128;
        let mut inner = self.service.inner.write().unwrap();
        let (nodes, tenants, scheduler) = inner.parts_mut();

        // Prefer to migrate tenants that are currently outside their home AZ.  This avoids the chaos injector
        // continuously pushing tenants outside their home AZ: instead, we'll tend to cycle between picking some
        // random tenants to move, and then on next chaos iteration moving them back, then picking some new
        // random tenants on the next iteration.
        let (out_of_home_az, in_home_az): (Vec<_>, Vec<_>) = tenants
            .values()
            .map(|shard| {
                (
                    shard.tenant_shard_id,
                    shard.is_attached_outside_preferred_az(nodes),
                )
            })
            .partition(|(_id, is_outside)| *is_outside);

        let mut out_of_home_az: Vec<_> = out_of_home_az.into_iter().map(|(id, _)| id).collect();
        let mut in_home_az: Vec<_> = in_home_az.into_iter().map(|(id, _)| id).collect();

        let mut victims = Vec::with_capacity(batch_size);
        if out_of_home_az.len() >= batch_size {
            tracing::info!(
                "Injecting chaos: found {batch_size} shards to migrate back to home AZ (total {} out of home AZ)",
                out_of_home_az.len()
            );

            out_of_home_az.shuffle(&mut thread_rng());
            victims.extend(out_of_home_az.into_iter().take(batch_size));
        } else {
            tracing::info!(
                "Injecting chaos: found {} shards to migrate back to home AZ, picking {} random shards to migrate",
                out_of_home_az.len(),
                std::cmp::min(batch_size - out_of_home_az.len(), in_home_az.len())
            );

            victims.extend(out_of_home_az);
            in_home_az.shuffle(&mut thread_rng());
            victims.extend(in_home_az.into_iter().take(batch_size - victims.len()));
        }

        for victim in victims {
            self.maybe_migrate_to_secondary(victim, nodes, tenants, scheduler);
        }
    }
}
