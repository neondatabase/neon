use std::collections::HashMap;

use control_plane::endpoint::ComputeControlPlane;
use control_plane::local_env::LocalEnv;
use pageserver_api::shard::{ShardIndex, TenantShardId};
use postgres_connection::parse_host_port;
use utils::id::{NodeId, TenantId};

pub(super) struct ComputeHookTenant {
    shards: Vec<(ShardIndex, NodeId)>,
}

impl ComputeHookTenant {
    pub(super) async fn maybe_reconfigure(&mut self, tenant_id: TenantId) -> anyhow::Result<()> {
        // Find the highest shard count and drop any shards that aren't
        // for that shard count.
        let shard_count = self.shards.iter().map(|(k, _v)| k.shard_count).max();
        let Some(shard_count) = shard_count else {
            // No shards, nothing to do.
            tracing::info!("ComputeHookTenant::maybe_reconfigure: no shards");
            return Ok(());
        };

        self.shards.retain(|(k, _v)| k.shard_count == shard_count);
        self.shards
            .sort_by_key(|(shard, _node_id)| shard.shard_number);

        if self.shards.len() == shard_count.0 as usize {
            // We have pageservers for all the shards: proceed to reconfigure compute
            let env = LocalEnv::load_config().expect("Error loading config");
            let cplane = ComputeControlPlane::load(env.clone())
                .expect("Error loading compute control plane");

            let compute_pageservers = self
                .shards
                .iter()
                .map(|(_shard, node_id)| {
                    let ps_conf = env
                        .get_pageserver_conf(*node_id)
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
        } else {
            tracing::info!(
                "ComputeHookTenant::maybe_reconfigure: not enough shards ({}/{})",
                self.shards.len(),
                shard_count.0
            );
        }

        Ok(())
    }
}

/// The compute hook is a destination for notifications about changes to tenant:pageserver
/// mapping.  It aggregates updates for the shards in a tenant, and when appropriate reconfigures
/// the compute connection string.
pub(super) struct ComputeHook {
    state: tokio::sync::Mutex<HashMap<TenantId, ComputeHookTenant>>,
}

impl ComputeHook {
    pub(super) fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    pub(super) async fn notify(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        tracing::info!("ComputeHook::notify: {}->{}", tenant_shard_id, node_id);
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

        entry.maybe_reconfigure(tenant_shard_id.tenant_id).await
    }
}
