use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use pageserver_api::controller_api::ShardSchedulingPolicy;
use rand::seq::SliceRandom;
use rand::{Rng, thread_rng};
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
            MigrationsToSecondary,
            ForceKillController,
            GracefulMigrationsAnywhere,
        }
        loop {
            let cron_interval = self.get_cron_interval_sleep_future();
            let chaos_type = tokio::select! {
                _ = interval.tick() => {
                    if thread_rng().gen_bool(0.5) {
                        ChaosEvent::MigrationsToSecondary
                    } else {
                        ChaosEvent::GracefulMigrationsAnywhere
                    }
                }
                Some(_) = maybe_sleep(cron_interval) => {
                    ChaosEvent::ForceKillController
                }
                _ = cancel.cancelled() => {
                    tracing::info!("Shutting down");
                    return;
                }
            };
            tracing::info!("Chaos iteration: {chaos_type:?}...");
            match chaos_type {
                ChaosEvent::MigrationsToSecondary => {
                    self.inject_migrations_to_secondary();
                }
                ChaosEvent::GracefulMigrationsAnywhere => {
                    self.inject_graceful_migrations_anywhere();
                }
                ChaosEvent::ForceKillController => {
                    self.force_kill().await;
                }
            }
        }
    }

    fn is_shard_eligible_for_chaos(&self, shard: &TenantShard) -> bool {
        // - Skip non-active scheduling policies, so that a shard with a policy like Pause can
        //   be pinned without being disrupted by us.
        // - Skip shards doing a graceful migration already, so that we allow these to run to
        //   completion rather than only exercising the first part and then cancelling with
        //   some other chaos.
        matches!(shard.get_scheduling_policy(), ShardSchedulingPolicy::Active)
            && shard.get_preferred_node().is_none()
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

        if !self.is_shard_eligible_for_chaos(shard) {
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

    // Unlike [`Self::inject_migrations_to_secondary`], this function will not only cut over to secondary, it
    // will migrate a tenant to a random node in its home AZ using a graceful migration of the same type
    // that my be initiated by an API caller using prewarm=true.
    //
    // This is a much more expensive operation in terms of I/O and time, as we will fully warm up
    // some new location in order to migrate the tenant there.  For that reason we do far fewer of these.
    fn inject_graceful_migrations_anywhere(&mut self) {
        let batch_size = 1;
        let mut inner = self.service.inner.write().unwrap();
        let (nodes, tenants, _scheduler) = inner.parts_mut();

        let mut candidates = tenants
            .values_mut()
            .filter(|shard| self.is_shard_eligible_for_chaos(shard))
            .collect::<Vec<_>>();

        tracing::info!(
            "Injecting chaos: found {} candidates for graceful migrations anywhere",
            candidates.len()
        );

        let mut victims: Vec<&mut TenantShard> = Vec::new();

        // Pick our victims: use a hand-rolled loop rather than choose_multiple() because we want
        // to take the mutable refs from our candidates rather than ref'ing them.
        while !candidates.is_empty() && victims.len() < batch_size {
            let i = thread_rng().gen_range(0..candidates.len());
            victims.push(candidates.swap_remove(i));
        }

        for victim in victims.into_iter() {
            // Find a node in the same AZ as the shard, or if the shard has no AZ preference, which
            // is not where they are currently attached.
            let candidate_nodes = nodes
                .values()
                .filter(|node| {
                    if let Some(preferred_az) = victim.preferred_az() {
                        node.get_availability_zone_id() == preferred_az
                    } else if let Some(attached) = *victim.intent.get_attached() {
                        node.get_id() != attached
                    } else {
                        true
                    }
                })
                .collect::<Vec<_>>();

            let Some(victim_node) = candidate_nodes.choose(&mut thread_rng()) else {
                // This can happen if e.g. we are in a small region with only one pageserver per AZ.
                tracing::info!(
                    "no candidate nodes found for migrating shard {tenant_shard_id} within its home AZ",
                    tenant_shard_id = victim.tenant_shard_id
                );
                continue;
            };

            // This doesn't change intent immediately: next iteration of Service::optimize_all should do that.  We avoid
            // doing it here because applying optimizations requires dropping lock to do some async work to check the optimisation
            // is valid given remote state, and it would be a shame to duplicate that dance here.
            tracing::info!(
                "Injecting chaos: migrate {} to {}",
                victim.tenant_shard_id,
                victim_node
            );
            victim.set_preferred_node(Some(victim_node.get_id()));
        }
    }

    /// Migrations of attached locations to their secondary location.  This exercises reconciliation in general,
    /// live migration in particular, and the pageserver code for cleanly shutting down and starting up tenants
    /// during such migrations.
    fn inject_migrations_to_secondary(&mut self) {
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
