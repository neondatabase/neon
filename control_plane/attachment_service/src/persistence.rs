use std::{collections::HashMap, str::FromStr};

use camino::{Utf8Path, Utf8PathBuf};
use control_plane::{
    attachment_service::{NodeAvailability, NodeSchedulingPolicy},
    local_env::LocalEnv,
};
use pageserver_api::{
    models::TenantConfig,
    shard::{ShardCount, ShardNumber, TenantShardId},
};
use postgres_connection::parse_host_port;
use serde::{Deserialize, Serialize};
use utils::{
    generation::Generation,
    id::{NodeId, TenantId},
};

use crate::{node::Node, PlacementPolicy};

/// Placeholder for storage.  This will be replaced with a database client.
pub struct Persistence {
    state: std::sync::Mutex<PersistentState>,
}

// Top level state available to all HTTP handlers
#[derive(Serialize, Deserialize)]
struct PersistentState {
    tenants: HashMap<TenantShardId, TenantShardPersistence>,

    #[serde(skip)]
    path: Utf8PathBuf,
}

/// A convenience for serializing the state inside a sync lock, and then
/// writing it to disk outside of the lock.  This will go away when switching
/// to a database backend.
struct PendingWrite {
    bytes: Vec<u8>,
    path: Utf8PathBuf,
}

impl PendingWrite {
    async fn commit(self) -> anyhow::Result<()> {
        tokio::task::spawn_blocking(|| {
            let tmp_path = utils::crashsafe::path_with_suffix_extension(self.path, "___new");
            utils::crashsafe::overwrite(&self.path, &tmp_path, &self.bytes)
        })
        .await
        .context("spawn_blocking")?
        .context("write file")
    }
}

impl PersistentState {
    fn save(&self) -> PendingWrite {
        PendingWrite {
            bytes: serde_json::to_vec(self).expect("Serialization error"),
            path: self.path.clone(),
        }
    }

    async fn load(path: &Utf8Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        let mut decoded = serde_json::from_slice::<Self>(&bytes)?;
        decoded.path = path.to_owned();

        for (tenant_id, tenant) in &mut decoded.tenants {
            // Backward compat: an old attachments.json from before PR #6251, replace
            // empty strings with proper defaults.
            if tenant.tenant_id.is_empty() {
                tenant.tenant_id = format!("{}", tenant_id);
                tenant.config = serde_json::to_string(&TenantConfig::default())?;
                tenant.placement_policy = serde_json::to_string(&PlacementPolicy::default())?;
            }
        }

        Ok(decoded)
    }

    async fn load_or_new(path: &Utf8Path) -> Self {
        match Self::load(path).await {
            Ok(s) => {
                tracing::info!("Loaded state file at {}", path);
                s
            }
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .map(|e| e.kind() == std::io::ErrorKind::NotFound)
                    .unwrap_or(false) =>
            {
                tracing::info!("Will create state file at {}", path);
                Self {
                    tenants: HashMap::new(),
                    path: path.to_owned(),
                }
            }
            Err(e) => {
                panic!("Failed to load state from '{}': {e:#} (maybe your .neon/ dir was written by an older version?)", path)
            }
        }
    }
}

impl Persistence {
    pub async fn new(path: &Utf8Path) -> Self {
        let state = PersistentState::load_or_new(path).await;
        Self {
            state: std::sync::Mutex::new(state),
        }
    }

    /// When registering a node, persist it so that on next start we will be able to
    /// iterate over known nodes to synchronize their tenant shard states with our observed state.
    pub(crate) async fn insert_node(&self, _node: &Node) -> anyhow::Result<()> {
        // TODO: node persitence will come with database backend
        Ok(())
    }

    /// At startup, we populate the service's list of nodes, and use this list to call into
    /// each node to do an initial reconciliation of the state of the world with our in-memory
    /// observed state.
    pub(crate) async fn list_nodes(&self) -> anyhow::Result<Vec<Node>> {
        let env = LocalEnv::load_config()?;
        // TODO: node persitence will come with database backend

        // XXX hack: enable test_backward_compatibility to work by populating our list of
        // nodes from LocalEnv when it is not present in persistent storage.  Otherwise at
        // first startup in the compat test, we may have shards but no nodes.
        let mut result = Vec::new();
        tracing::info!(
            "Loaded {} pageserver nodes from LocalEnv",
            env.pageservers.len()
        );
        for ps_conf in env.pageservers {
            let (pg_host, pg_port) =
                parse_host_port(&ps_conf.listen_pg_addr).expect("Unable to parse listen_pg_addr");
            let (http_host, http_port) = parse_host_port(&ps_conf.listen_http_addr)
                .expect("Unable to parse listen_http_addr");
            result.push(Node {
                id: ps_conf.id,
                listen_pg_addr: pg_host.to_string(),
                listen_pg_port: pg_port.unwrap_or(5432),
                listen_http_addr: http_host.to_string(),
                listen_http_port: http_port.unwrap_or(80),
                availability: NodeAvailability::Active,
                scheduling: NodeSchedulingPolicy::Active,
            });
        }

        Ok(result)
    }

    /// At startup, we populate our map of tenant shards from persistent storage.
    pub(crate) async fn list_tenant_shards(&self) -> anyhow::Result<Vec<TenantShardPersistence>> {
        let locked = self.state.lock().unwrap();
        Ok(locked.tenants.values().cloned().collect())
    }

    /// Tenants must be persisted before we schedule them for the first time.  This enables us
    /// to correctly retain generation monotonicity, and the externally provided placement policy & config.
    pub(crate) async fn insert_tenant_shards(
        &self,
        shards: Vec<TenantShardPersistence>,
    ) -> anyhow::Result<()> {
        let write = {
            let mut locked = self.state.lock().unwrap();
            for shard in shards {
                let tenant_shard_id = TenantShardId {
                    tenant_id: TenantId::from_str(shard.tenant_id.as_str())?,
                    shard_number: ShardNumber(shard.shard_number as u8),
                    shard_count: ShardCount(shard.shard_count as u8),
                };

                locked.tenants.insert(tenant_shard_id, shard);
            }
            locked.save()
        };

        write.commit().await?;

        Ok(())
    }

    /// Reconciler calls this immediately before attaching to a new pageserver, to acquire a unique, monotonically
    /// advancing generation number.  We also store the NodeId for which the generation was issued, so that in
    /// [`Self::re_attach`] we can do a bulk UPDATE on the generations for that node.
    pub(crate) async fn increment_generation(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<Generation> {
        let (write, gen) = {
            let mut locked = self.state.lock().unwrap();
            let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) else {
                anyhow::bail!("Tried to increment generation of unknown shard");
            };

            shard.generation += 1;
            shard.generation_pageserver = Some(node_id);

            let gen = Generation::new(shard.generation);
            (locked.save(), gen)
        };

        write.commit().await?;
        Ok(gen)
    }

    pub(crate) async fn detach(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        let write = {
            let mut locked = self.state.lock().unwrap();
            let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) else {
                anyhow::bail!("Tried to increment generation of unknown shard");
            };
            shard.generation_pageserver = None;
            locked.save()
        };
        write.commit().await?;
        Ok(())
    }

    pub(crate) async fn re_attach(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<HashMap<TenantShardId, Generation>> {
        let (write, result) = {
            let mut result = HashMap::new();
            let mut locked = self.state.lock().unwrap();
            for (tenant_shard_id, shard) in locked.tenants.iter_mut() {
                if shard.generation_pageserver == Some(node_id) {
                    shard.generation += 1;
                    result.insert(*tenant_shard_id, Generation::new(shard.generation));
                }
            }

            (locked.save(), result)
        };

        write.commit().await?;
        Ok(result)
    }

    // TODO: when we start shard splitting, we must durably mark the tenant so that
    // on restart, we know that we must go through recovery (list shards that exist
    // and pick up where we left off and/or revert to parent shards).
    #[allow(dead_code)]
    pub(crate) async fn begin_shard_split(&self, _tenant_id: TenantId) -> anyhow::Result<()> {
        todo!();
    }

    // TODO: when we finish shard splitting, we must atomically clean up the old shards
    // and insert the new shards, and clear the splitting marker.
    #[allow(dead_code)]
    pub(crate) async fn complete_shard_split(&self, _tenant_id: TenantId) -> anyhow::Result<()> {
        todo!();
    }
}

/// Parts of [`crate::tenant_state::TenantState`] that are stored durably
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TenantShardPersistence {
    #[serde(default)]
    pub(crate) tenant_id: String,
    #[serde(default)]
    pub(crate) shard_number: i32,
    #[serde(default)]
    pub(crate) shard_count: i32,
    #[serde(default)]
    pub(crate) shard_stripe_size: i32,

    // Currently attached pageserver
    #[serde(rename = "pageserver")]
    pub(crate) generation_pageserver: Option<NodeId>,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching
    pub(crate) generation: u32,

    #[serde(default)]
    pub(crate) placement_policy: String,
    #[serde(default)]
    pub(crate) config: String,
}
