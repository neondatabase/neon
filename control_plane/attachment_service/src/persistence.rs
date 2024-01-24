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
use tracing::info;
use utils::{
    generation::Generation,
    id::{NodeId, TenantId},
};

use crate::{node::Node, PlacementPolicy};

/// Placeholder for storage.  This will be replaced with a database client.
pub struct Persistence {
    inner: std::sync::Mutex<Inner>,
}

struct Inner {
    state: PersistentState,
    write_queue_tx: tokio::sync::mpsc::UnboundedSender<PendingWrite>,
}

#[derive(Serialize, Deserialize)]
struct PersistentState {
    tenants: HashMap<TenantShardId, TenantShardPersistence>,
}

struct PendingWrite {
    bytes: Vec<u8>,
    done_tx: tokio::sync::oneshot::Sender<()>,
}

impl PersistentState {
    async fn load(path: &Utf8Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        let mut decoded = serde_json::from_slice::<Self>(&bytes)?;

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
                }
            }
            Err(e) => {
                panic!("Failed to load state from '{}': {e:#} (maybe your .neon/ dir was written by an older version?)", path)
            }
        }
    }
}

impl Persistence {
    pub async fn spawn(path: &Utf8Path) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let state = PersistentState::load_or_new(path).await;
        tokio::spawn(Self::writer_task(rx, path.to_owned()));
        Self {
            inner: std::sync::Mutex::new(Inner {
                state,
                write_queue_tx: tx,
            }),
        }
    }

    async fn writer_task(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingWrite>,
        path: Utf8PathBuf,
    ) {
        scopeguard::defer! {
            info!("persistence writer task exiting");
        };
        loop {
            match rx.recv().await {
                Some(write) => {
                    tokio::task::spawn_blocking({
                        let path = path.clone();
                        move || {
                            let tmp_path =
                                utils::crashsafe::path_with_suffix_extension(&path, "___new");
                            utils::crashsafe::overwrite(&path, &tmp_path, &write.bytes)
                        }
                    })
                    .await
                    .expect("spawn_blocking")
                    .expect("write file");
                    let _ = write.done_tx.send(()); // receiver may lose interest any time
                }
                None => {
                    return;
                }
            }
        }
    }

    /// Perform a modification on our [`PersistentState`].
    /// Return a future that completes once our modification has been persisted.
    /// The output of the future is the return value of the `txn`` closure.
    async fn mutating_transaction<F, R>(&self, txn: F) -> R
    where
        F: FnOnce(&mut PersistentState) -> R,
    {
        let (ret, done_rx) = {
            let mut inner = self.inner.lock().unwrap();
            let ret = txn(&mut inner.state);
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let write = PendingWrite {
                bytes: serde_json::to_vec(&inner.state).expect("Serialization error"),
                done_tx,
            };
            inner
                .write_queue_tx
                .send(write)
                .expect("writer task always outlives self");
            (ret, done_rx)
        };
        // the write task can go away once we start .await'ing
        let _: () = done_rx.await.expect("writer task dead, check logs");
        ret
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
        let inner = self.inner.lock().unwrap();
        Ok(inner.state.tenants.values().cloned().collect())
    }

    /// Tenants must be persisted before we schedule them for the first time.  This enables us
    /// to correctly retain generation monotonicity, and the externally provided placement policy & config.
    pub(crate) async fn insert_tenant_shards(
        &self,
        shards: Vec<TenantShardPersistence>,
    ) -> anyhow::Result<()> {
        self.mutating_transaction(|locked| {
            for shard in shards {
                let tenant_shard_id = TenantShardId {
                    tenant_id: TenantId::from_str(shard.tenant_id.as_str())?,
                    shard_number: ShardNumber(shard.shard_number as u8),
                    shard_count: ShardCount(shard.shard_count as u8),
                };

                locked.tenants.insert(tenant_shard_id, shard);
            }
            Ok(())
        })
        .await
    }

    /// Reconciler calls this immediately before attaching to a new pageserver, to acquire a unique, monotonically
    /// advancing generation number.  We also store the NodeId for which the generation was issued, so that in
    /// [`Self::re_attach`] we can do a bulk UPDATE on the generations for that node.
    pub(crate) async fn increment_generation(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<Generation> {
        self.mutating_transaction(|locked| {
            let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) else {
                anyhow::bail!("Tried to increment generation of unknown shard");
            };

            shard.generation += 1;
            shard.generation_pageserver = Some(node_id);

            let gen = Generation::new(shard.generation);
            Ok(gen)
        })
        .await
    }

    pub(crate) async fn detach(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        self.mutating_transaction(|locked| {
            let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) else {
                anyhow::bail!("Tried to increment generation of unknown shard");
            };
            shard.generation_pageserver = None;
            shard.placement_policy = serde_json::to_string(&PlacementPolicy::Detached).unwrap();
            Ok(())
        })
        .await
    }

    pub(crate) async fn re_attach(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<HashMap<TenantShardId, Generation>> {
        self.mutating_transaction(|locked| {
            let mut result = HashMap::new();
            for (tenant_shard_id, shard) in locked.tenants.iter_mut() {
                if shard.generation_pageserver == Some(node_id) {
                    shard.generation += 1;
                    result.insert(*tenant_shard_id, Generation::new(shard.generation));
                }
            }
            Ok(result)
        })
        .await
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
