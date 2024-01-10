use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use camino::Utf8Path;
use camino::Utf8PathBuf;
use control_plane::attachment_service::{NodeAvailability, NodeSchedulingPolicy};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::Connection;
use pageserver_api::models::TenantConfig;
use pageserver_api::shard::{ShardCount, ShardNumber, TenantShardId};
use postgres_connection::parse_host_port;
use serde::{Deserialize, Serialize};
use utils::generation::Generation;
use utils::id::{NodeId, TenantId};

use crate::node::Node;
use crate::PlacementPolicy;

/// ## What do we store?
///
/// The attachment service does not store most of its state durably.
///
/// The essential things to store durably are:
/// - generation numbers, as these must always advance monotonically to ensure data safety.
/// - Tenant's PlacementPolicy and TenantConfig, as the source of truth for these is something external.
/// - Node's scheduling policies, as the source of truth for these is something external.
///
/// Other things we store durably as an implementation detail:
/// - Node's host/port: this could be avoided it we made nodes emit a self-registering heartbeat,
///   but it is operationally simpler to make this service the authority for which nodes
///   it talks to.
///
/// ## Performance/efficiency
///
/// The attachment service does not go via the database for most things: there are
/// a couple of places where we must, and where efficiency matters:
/// - Incrementing generation numbers: the Reconciler has to wait for this to complete
///   before it can attach a tenant, so this acts as a bound on how fast things like
///   failover can happen.
/// - Pageserver re-attach: we will increment many shards' generations when this happens,
///   so it is important to avoid e.g. issuing O(N) queries.
///
/// Database calls relating to nodes have low performance requirements, as they are very rarely
/// updated, and reads of nodes are always from memory, not the database.  We only require that
/// we can UPDATE a node's scheduling mode reasonably quickly to mark a bad node offline.
pub struct Persistence {
    database_url: String,

    // In test environments, we support loading+saving a JSON file.  This is temporary, for the benefit of
    // test_compatibility.py, so that we don't have to commit to making the database contents fully backward/forward
    // compatible just yet.
    json_path: Option<Utf8PathBuf>,
}

/// Legacy format, for use in JSON compat objects in test environment
#[derive(Serialize, Deserialize)]
struct JsonPersistence {
    tenants: HashMap<TenantShardId, TenantShardPersistence>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum DatabaseError {
    #[error(transparent)]
    Query(#[from] diesel::result::Error),
    #[error(transparent)]
    Connection(#[from] diesel::result::ConnectionError),
    #[error("Logical error: {0}")]
    Logical(String),
}

pub(crate) type DatabaseResult<T> = Result<T, DatabaseError>;

impl Persistence {
    pub fn new(json_path: Option<Utf8PathBuf>) -> Self {
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        Self {
            database_url,
            json_path,
        }
    }

    /// Call the provided function in a tokio blocking thread, with a Diesel database connection.
    async fn with_conn<F, R>(&self, func: F) -> DatabaseResult<R>
    where
        F: Fn(&mut PgConnection) -> DatabaseResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let database_url = self.database_url.clone();
        tokio::task::spawn_blocking(move || -> DatabaseResult<R> {
            // TODO: connection pooling, such as via diesel::r2d2
            let mut conn = PgConnection::establish(&database_url)?;
            func(&mut conn)
        })
        .await
        .expect("Task panic")
    }

    /// When a node is first registered, persist it before using it for anything
    pub(crate) async fn insert_node(&self, node: &Node) -> DatabaseResult<()> {
        let np = node.to_persistent();
        self.with_conn(move |conn| -> DatabaseResult<()> {
            diesel::insert_into(crate::schema::nodes::table)
                .values(&np)
                .execute(conn)?;
            Ok(())
        })
        .await
    }

    /// At startup, populate the list of nodes which our shards may be placed on
    pub(crate) async fn list_nodes(&self) -> DatabaseResult<Vec<Node>> {
        let nodes: Vec<Node> = self
            .with_conn(move |conn| -> DatabaseResult<_> {
                Ok(crate::schema::nodes::table
                    .load::<NodePersistence>(conn)?
                    .into_iter()
                    .map(|n| Node {
                        id: NodeId(n.node_id as u64),
                        // At startup we consider a node offline until proven otherwise.
                        availability: NodeAvailability::Offline,
                        scheduling: NodeSchedulingPolicy::from_str(&n.scheduling_policy)
                            .expect("Bad scheduling policy in DB"),
                        listen_http_addr: n.listen_http_addr,
                        listen_http_port: n.listen_http_port as u16,
                        listen_pg_addr: n.listen_pg_addr,
                        listen_pg_port: n.listen_pg_port as u16,
                    })
                    .collect::<Vec<Node>>())
            })
            .await?;

        if nodes.is_empty() {
            return self.list_nodes_local_env().await;
        }

        tracing::info!("list_nodes: loaded {} nodes", nodes.len());

        Ok(nodes)
    }

    /// Shim for automated compatibility tests: load nodes from LocalEnv instead of database
    pub(crate) async fn list_nodes_local_env(&self) -> DatabaseResult<Vec<Node>> {
        // Enable test_backward_compatibility to work by populating our list of
        // nodes from LocalEnv when it is not present in persistent storage.  Otherwise at
        // first startup in the compat test, we may have shards but no nodes.
        use control_plane::local_env::LocalEnv;
        let env = LocalEnv::load_config().map_err(|e| DatabaseError::Logical(format!("{e}")))?;
        tracing::info!(
            "Loading {} pageserver nodes from LocalEnv",
            env.pageservers.len()
        );
        let mut nodes = Vec::new();
        for ps_conf in env.pageservers {
            let (pg_host, pg_port) =
                parse_host_port(&ps_conf.listen_pg_addr).expect("Unable to parse listen_pg_addr");
            let (http_host, http_port) = parse_host_port(&ps_conf.listen_http_addr)
                .expect("Unable to parse listen_http_addr");
            let node = Node {
                id: ps_conf.id,
                listen_pg_addr: pg_host.to_string(),
                listen_pg_port: pg_port.unwrap_or(5432),
                listen_http_addr: http_host.to_string(),
                listen_http_port: http_port.unwrap_or(80),
                availability: NodeAvailability::Active,
                scheduling: NodeSchedulingPolicy::Active,
            };

            // Synchronize database with what we learn from LocalEnv
            self.insert_node(&node).await?;

            nodes.push(node);
        }

        Ok(nodes)
    }

    /// At startup, load the high level state for shards, such as their config + policy.  This will
    /// be enriched at runtime with state discovered on pageservers.
    pub(crate) async fn list_tenant_shards(&self) -> DatabaseResult<Vec<TenantShardPersistence>> {
        let loaded = self
            .with_conn(move |conn| -> DatabaseResult<_> {
                Ok(crate::schema::tenant_shards::table.load::<TenantShardPersistence>(conn)?)
            })
            .await?;

        if loaded.is_empty() {
            if let Some(path) = &self.json_path {
                if tokio::fs::try_exists(path)
                    .await
                    .map_err(|e| DatabaseError::Logical(format!("Error stat'ing JSON file: {e}")))?
                {
                    tracing::info!("Importing from legacy JSON format at {path}");
                    return self.list_tenant_shards_json(path).await;
                }
            }
        }
        Ok(loaded)
    }

    /// Shim for automated compatibility tests: load tenants from a JSON file instead of database
    pub(crate) async fn list_tenant_shards_json(
        &self,
        path: &Utf8Path,
    ) -> DatabaseResult<Vec<TenantShardPersistence>> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| DatabaseError::Logical(format!("Failed to load JSON: {e}")))?;

        let mut decoded = serde_json::from_slice::<JsonPersistence>(&bytes)
            .map_err(|e| DatabaseError::Logical(format!("Deserialization error: {e}")))?;
        for (tenant_id, tenant) in &mut decoded.tenants {
            // Backward compat: an old attachments.json from before PR #6251, replace
            // empty strings with proper defaults.
            if tenant.tenant_id.is_empty() {
                tenant.tenant_id = format!("{}", tenant_id);
                tenant.config = serde_json::to_string(&TenantConfig::default())
                    .map_err(|e| DatabaseError::Logical(format!("Serialization error: {e}")))?;
                tenant.placement_policy = serde_json::to_string(&PlacementPolicy::default())
                    .map_err(|e| DatabaseError::Logical(format!("Serialization error: {e}")))?;
            }
        }

        let tenants: Vec<TenantShardPersistence> = decoded.tenants.into_values().collect();

        // Synchronize database with what is in the JSON file
        self.insert_tenant_shards(tenants.clone()).await?;

        Ok(tenants)
    }

    /// For use in testing environments, where we dump out JSON on shutdown.
    pub async fn write_tenants_json(&self) -> anyhow::Result<()> {
        let Some(path) = &self.json_path else {
            anyhow::bail!("Cannot write JSON if path isn't set (test environment bug)");
        };
        tracing::info!("Writing state to {path}...");
        let tenants = self.list_tenant_shards().await?;
        let mut tenants_map = HashMap::new();
        for tsp in tenants {
            let tenant_shard_id = TenantShardId {
                tenant_id: TenantId::from_str(tsp.tenant_id.as_str())?,
                shard_number: ShardNumber(tsp.shard_number as u8),
                shard_count: ShardCount(tsp.shard_count as u8),
            };

            tenants_map.insert(tenant_shard_id, tsp);
        }
        let json = serde_json::to_string(&JsonPersistence {
            tenants: tenants_map,
        })?;

        tokio::fs::write(path, &json).await?;
        tracing::info!("Wrote {} bytes to {path}...", json.len());

        Ok(())
    }

    /// Tenants must be persisted before we schedule them for the first time.  This enables us
    /// to correctly retain generation monotonicity, and the externally provided placement policy & config.
    pub(crate) async fn insert_tenant_shards(
        &self,
        shards: Vec<TenantShardPersistence>,
    ) -> DatabaseResult<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_conn(move |conn| -> DatabaseResult<()> {
            conn.transaction(|conn| -> QueryResult<()> {
                for tenant in &shards {
                    diesel::insert_into(tenant_shards)
                        .values(tenant)
                        .execute(conn)?;
                }
                Ok(())
            })?;
            Ok(())
        })
        .await
    }

    /// Ordering: call this _after_ deleting the tenant on pageservers, but _before_ dropping state for
    /// the tenant from memory on this server.
    #[allow(unused)]
    pub(crate) async fn delete_tenant(&self, del_tenant_id: TenantId) -> DatabaseResult<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_conn(move |conn| -> DatabaseResult<()> {
            diesel::delete(tenant_shards)
                .filter(tenant_id.eq(del_tenant_id.to_string()))
                .execute(conn)?;

            Ok(())
        })
        .await
    }

    /// When a tenant invokes the /re-attach API, this function is responsible for doing an efficient
    /// batched increment of the generations of all tenants whose generation_pageserver is equal to
    /// the node that called /re-attach.
    #[tracing::instrument(skip_all, fields(node_id))]
    pub(crate) async fn re_attach(
        &self,
        node_id: NodeId,
    ) -> DatabaseResult<HashMap<TenantShardId, Generation>> {
        use crate::schema::tenant_shards::dsl::*;
        let updated = self
            .with_conn(move |conn| {
                let rows_updated = diesel::update(tenant_shards)
                    .filter(generation_pageserver.eq(node_id.0 as i64))
                    .set(generation.eq(generation + 1))
                    .execute(conn)?;

                tracing::info!("Incremented {} tenants' generations", rows_updated);

                // TODO: UPDATE+SELECT in one query

                let updated = tenant_shards
                    .filter(generation_pageserver.eq(node_id.0 as i64))
                    .select(TenantShardPersistence::as_select())
                    .load(conn)?;
                Ok(updated)
            })
            .await?;

        let mut result = HashMap::new();
        for tsp in updated {
            let tenant_shard_id = TenantShardId {
                tenant_id: TenantId::from_str(tsp.tenant_id.as_str())
                    .map_err(|e| DatabaseError::Logical(format!("Malformed tenant id: {e}")))?,
                shard_number: ShardNumber(tsp.shard_number as u8),
                shard_count: ShardCount(tsp.shard_count as u8),
            };
            result.insert(tenant_shard_id, Generation::new(tsp.generation as u32));
        }

        Ok(result)
    }

    /// Reconciler calls this immediately before attaching to a new pageserver, to acquire a unique, monotonically
    /// advancing generation number.  We also store the NodeId for which the generation was issued, so that in
    /// [`Self::re_attach`] we can do a bulk UPDATE on the generations for that node.
    pub(crate) async fn increment_generation(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<Generation> {
        use crate::schema::tenant_shards::dsl::*;
        let updated = self
            .with_conn(move |conn| {
                let updated = diesel::update(tenant_shards)
                    .filter(tenant_id.eq(tenant_shard_id.tenant_id.to_string()))
                    .filter(shard_number.eq(tenant_shard_id.shard_number.0 as i32))
                    .filter(shard_count.eq(tenant_shard_id.shard_count.0 as i32))
                    .set((
                        generation.eq(generation + 1),
                        generation_pageserver.eq(node_id.0 as i64),
                    ))
                    // TODO: only returning() the generation column
                    .returning(TenantShardPersistence::as_returning())
                    .get_result(conn)?;

                Ok(updated)
            })
            .await?;

        Ok(Generation::new(updated.generation as u32))
    }

    pub(crate) async fn detach(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_conn(move |conn| {
            let updated = diesel::update(tenant_shards)
                .filter(tenant_id.eq(tenant_shard_id.tenant_id.to_string()))
                .filter(shard_number.eq(tenant_shard_id.shard_number.0 as i32))
                .filter(shard_count.eq(tenant_shard_id.shard_count.0 as i32))
                .set((
                    generation_pageserver.eq(i64::MAX),
                    placement_policy.eq(serde_json::to_string(&PlacementPolicy::Detached).unwrap()),
                ))
                .execute(conn)?;

            Ok(updated)
        })
        .await?;

        Ok(())
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
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Clone)]
#[diesel(table_name = crate::schema::tenant_shards)]
pub(crate) struct TenantShardPersistence {
    #[serde(default)]
    pub(crate) tenant_id: String,
    #[serde(default)]
    pub(crate) shard_number: i32,
    #[serde(default)]
    pub(crate) shard_count: i32,
    #[serde(default)]
    pub(crate) shard_stripe_size: i32,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching
    pub(crate) generation: i32,

    // Currently attached pageserver
    #[serde(rename = "pageserver")]
    pub(crate) generation_pageserver: i64,

    #[serde(default)]
    pub(crate) placement_policy: String,
    #[serde(default)]
    pub(crate) config: String,
}

/// Parts of [`crate::node::Node`] that are stored durably
#[derive(Serialize, Deserialize, Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::schema::nodes)]
pub(crate) struct NodePersistence {
    pub(crate) node_id: i64,
    pub(crate) scheduling_policy: String,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
}
