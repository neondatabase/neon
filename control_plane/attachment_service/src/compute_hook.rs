use std::collections::HashMap;

use control_plane::endpoint::ComputeControlPlane;
use control_plane::local_env::LocalEnv;
use pageserver_api::shard::{ShardCount, ShardIndex, ShardNumber, TenantShardId};
use postgres_connection::parse_host_port;
use serde::{Deserialize, Serialize};
use utils::id::{NodeId, TenantId};

use crate::service::Config;

pub(super) struct ComputeHookTenant {
    shards: Vec<(ShardIndex, NodeId)>,
}

#[derive(Serialize, Deserialize)]
struct ComputeHookNotifyRequestShard {
    node_id: NodeId,
    shard_number: ShardNumber,
}

/// Request body that we send to the control plane to notify it of where a tenant is attached
#[derive(Serialize, Deserialize)]
struct ComputeHookNotifyRequest {
    tenant_id: TenantId,
    shards: Vec<ComputeHookNotifyRequestShard>,
}

impl ComputeHookTenant {
    async fn maybe_reconfigure(
        &mut self,
        tenant_id: TenantId,
    ) -> anyhow::Result<Option<ComputeHookNotifyRequest>> {
        // Find the highest shard count and drop any shards that aren't
        // for that shard count.
        let shard_count = self.shards.iter().map(|(k, _v)| k.shard_count).max();
        let Some(shard_count) = shard_count else {
            // No shards, nothing to do.
            tracing::info!("ComputeHookTenant::maybe_reconfigure: no shards");
            return Ok(None);
        };

        self.shards.retain(|(k, _v)| k.shard_count == shard_count);
        self.shards
            .sort_by_key(|(shard, _node_id)| shard.shard_number);

        if self.shards.len() == shard_count.0 as usize || shard_count == ShardCount(0) {
            // We have pageservers for all the shards: emit a configuration update
            return Ok(Some(ComputeHookNotifyRequest {
                tenant_id,
                shards: self
                    .shards
                    .iter()
                    .map(|(shard, node_id)| ComputeHookNotifyRequestShard {
                        shard_number: shard.shard_number,
                        node_id: *node_id,
                    })
                    .collect(),
            }));
        } else {
            tracing::info!(
                "ComputeHookTenant::maybe_reconfigure: not enough shards ({}/{})",
                self.shards.len(),
                shard_count.0
            );
        }

        Ok(None)
    }
}

/// The compute hook is a destination for notifications about changes to tenant:pageserver
/// mapping.  It aggregates updates for the shards in a tenant, and when appropriate reconfigures
/// the compute connection string.
pub(super) struct ComputeHook {
    config: Config,
    state: tokio::sync::Mutex<HashMap<TenantId, ComputeHookTenant>>,
}

impl ComputeHook {
    pub(super) fn new(config: Config) -> Self {
        Self {
            state: Default::default(),
            config,
        }
    }

    /// For test environments: use neon_local's LocalEnv to update compute
    async fn do_notify_local(
        &self,
        reconfigure_request: ComputeHookNotifyRequest,
    ) -> anyhow::Result<()> {
        let env = match LocalEnv::load_config() {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Couldn't load neon_local config, skipping compute update ({e})");
                return Ok(());
            }
        };
        let cplane =
            ComputeControlPlane::load(env.clone()).expect("Error loading compute control plane");
        let ComputeHookNotifyRequest { tenant_id, shards } = reconfigure_request;

        let compute_pageservers = shards
            .into_iter()
            .map(|shard| {
                let ps_conf = env
                    .get_pageserver_conf(shard.node_id)
                    .expect("Unknown pageserver");
                let (pg_host, pg_port) = parse_host_port(&ps_conf.listen_pg_addr)
                    .expect("Unable to parse listen_pg_addr");
                (pg_host, pg_port.unwrap_or(5432))
            })
            .collect::<Vec<_>>();

        for (endpoint_name, endpoint) in &cplane.endpoints {
            if endpoint.tenant_id == tenant_id && endpoint.status() == "running" {
                tracing::info!("🔁 Reconfiguring endpoint {}", endpoint_name,);
                endpoint.reconfigure(compute_pageservers.clone()).await?;
            }
        }

        Ok(())
    }

    /// Call this to notify the compute (postgres) tier of new pageservers to use
    /// for a tenant.  notify() is called by each shard individually, and this function
    /// will decide whether an update to the tenant is sent.  An update is sent on the
    /// condition that:
    /// - We know a pageserver for every shard.
    /// - All the shards have the same shard_count (i.e. we are not mid-split)
    #[tracing::instrument(skip_all, fields(tenant_shard_id, node_id))]
    pub(super) async fn notify(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let mut locked = self.state.lock().await;
        let entry = locked
            .entry(tenant_shard_id.tenant_id)
            .or_insert_with(|| ComputeHookTenant { shards: Vec::new() });

        let shard_index = ShardIndex {
            shard_count: tenant_shard_id.shard_count,
            shard_number: tenant_shard_id.shard_number,
        };

        let mut set = false;
        for (existing_shard, existing_node) in &mut entry.shards {
            if *existing_shard == shard_index {
                *existing_node = node_id;
                set = true;
            }
        }
        if !set {
            entry.shards.push((shard_index, node_id));
        }

        let reconfigure_request = entry.maybe_reconfigure(tenant_shard_id.tenant_id).await?;
        let Some(reconfigure_request) = reconfigure_request else {
            // The tenant doesn't yet have pageservers for all its shards: we won't notify anything
            // until it does.
            tracing::debug!("Tenant isn't yet ready to emit a notification",);
            return Ok(());
        };

        if let Some(_notify_url) = &self.config.compute_hook_url {
            todo!();
        } else {
            self.do_notify_local(reconfigure_request).await
        }
    }
}
