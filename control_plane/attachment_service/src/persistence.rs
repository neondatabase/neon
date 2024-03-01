pub(crate) mod split_state;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use self::split_state::SplitState;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use diesel::pg::PgConnection;
use diesel::{
    Connection, ExpressionMethods, Insertable, QueryDsl, QueryResult, Queryable, RunQueryDsl,
    Selectable, SelectableHelper,
};
use pageserver_api::controller_api::NodeSchedulingPolicy;
use pageserver_api::models::TenantConfig;
use pageserver_api::shard::{ShardCount, ShardNumber, TenantShardId};
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
    connection_pool: diesel::r2d2::Pool<diesel::r2d2::ConnectionManager<PgConnection>>,

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
    #[error(transparent)]
    ConnectionPool(#[from] r2d2::Error),
    #[error("Logical error: {0}")]
    Logical(String),
}

pub(crate) type DatabaseResult<T> = Result<T, DatabaseError>;

impl Persistence {
    // The default postgres connection limit is 100.  We use up to 99, to leave one free for a human admin under
    // normal circumstances.  This assumes we have exclusive use of the database cluster to which we connect.
    pub const MAX_CONNECTIONS: u32 = 99;

    // We don't want to keep a lot of connections alive: close them down promptly if they aren't being used.
    const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(60);

    pub fn new(database_url: String, json_path: Option<Utf8PathBuf>) -> Self {
        let manager = diesel::r2d2::ConnectionManager::<PgConnection>::new(database_url);

        // We will use a connection pool: this is primarily to _limit_ our connection count, rather than to optimize time
        // to execute queries (database queries are not generally on latency-sensitive paths).
        let connection_pool = diesel::r2d2::Pool::builder()
            .max_size(Self::MAX_CONNECTIONS)
            .max_lifetime(Some(Self::MAX_CONNECTION_LIFETIME))
            .idle_timeout(Some(Self::IDLE_CONNECTION_TIMEOUT))
            // Always keep at least one connection ready to go
            .min_idle(Some(1))
            .test_on_check_out(true)
            .build(manager)
            .expect("Could not build connection pool");

        Self {
            connection_pool,
            json_path,
        }
    }

    /// Call the provided function in a tokio blocking thread, with a Diesel database connection.
    async fn with_conn<F, R>(&self, func: F) -> DatabaseResult<R>
    where
        F: Fn(&mut PgConnection) -> DatabaseResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let mut conn = self.connection_pool.get()?;
        tokio::task::spawn_blocking(move || -> DatabaseResult<R> { func(&mut conn) })
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
    pub(crate) async fn list_nodes(&self) -> DatabaseResult<Vec<NodePersistence>> {
        let nodes: Vec<NodePersistence> = self
            .with_conn(move |conn| -> DatabaseResult<_> {
                Ok(crate::schema::nodes::table.load::<NodePersistence>(conn)?)
            })
            .await?;

        tracing::info!("list_nodes: loaded {} nodes", nodes.len());

        Ok(nodes)
    }

    pub(crate) async fn update_node(
        &self,
        input_node_id: NodeId,
        input_scheduling: NodeSchedulingPolicy,
    ) -> DatabaseResult<()> {
        use crate::schema::nodes::dsl::*;
        let updated = self
            .with_conn(move |conn| {
                let updated = diesel::update(nodes)
                    .filter(node_id.eq(input_node_id.0 as i64))
                    .set((scheduling_policy.eq(String::from(input_scheduling)),))
                    .execute(conn)?;
                Ok(updated)
            })
            .await?;

        if updated != 1 {
            Err(DatabaseError::Logical(format!(
                "Node {node_id:?} not found for update",
            )))
        } else {
            Ok(())
        }
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
                tenant.tenant_id = tenant_id.to_string();
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
                shard_count: ShardCount::new(tsp.shard_count as u8),
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

    pub(crate) async fn delete_node(&self, del_node_id: NodeId) -> DatabaseResult<()> {
        use crate::schema::nodes::dsl::*;
        self.with_conn(move |conn| -> DatabaseResult<()> {
            diesel::delete(nodes)
                .filter(node_id.eq(del_node_id.0 as i64))
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
                shard_count: ShardCount::new(tsp.shard_count as u8),
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
                    .filter(shard_count.eq(tenant_shard_id.shard_count.literal() as i32))
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
                .filter(shard_count.eq(tenant_shard_id.shard_count.literal() as i32))
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

    // When we start shard splitting, we must durably mark the tenant so that
    // on restart, we know that we must go through recovery.
    //
    // We create the child shards here, so that they will be available for increment_generation calls
    // if some pageserver holding a child shard needs to restart before the overall tenant split is complete.
    pub(crate) async fn begin_shard_split(
        &self,
        old_shard_count: ShardCount,
        split_tenant_id: TenantId,
        parent_to_children: Vec<(TenantShardId, Vec<TenantShardPersistence>)>,
    ) -> DatabaseResult<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_conn(move |conn| -> DatabaseResult<()> {
            conn.transaction(|conn| -> DatabaseResult<()> {
                // Mark parent shards as splitting

                let updated = diesel::update(tenant_shards)
                    .filter(tenant_id.eq(split_tenant_id.to_string()))
                    .filter(shard_count.eq(old_shard_count.literal() as i32))
                    .set((splitting.eq(1),))
                    .execute(conn)?;
                if u8::try_from(updated)
                    .map_err(|_| DatabaseError::Logical(
                        format!("Overflow existing shard count {} while splitting", updated))
                    )? != old_shard_count.count() {
                    // Perhaps a deletion or another split raced with this attempt to split, mutating
                    // the parent shards that we intend to split. In this case the split request should fail.
                    return Err(DatabaseError::Logical(
                        format!("Unexpected existing shard count {updated} when preparing tenant for split (expected {})", old_shard_count.count())
                    ));
                }

                // FIXME: spurious clone to sidestep closure move rules
                let parent_to_children = parent_to_children.clone();

                // Insert child shards
                for (parent_shard_id, children) in parent_to_children {
                    let mut parent = crate::schema::tenant_shards::table
                        .filter(tenant_id.eq(parent_shard_id.tenant_id.to_string()))
                        .filter(shard_number.eq(parent_shard_id.shard_number.0 as i32))
                        .filter(shard_count.eq(parent_shard_id.shard_count.literal() as i32))
                        .load::<TenantShardPersistence>(conn)?;
                    let parent = if parent.len() != 1 {
                        return Err(DatabaseError::Logical(format!(
                            "Parent shard {parent_shard_id} not found"
                        )));
                    } else {
                        parent.pop().unwrap()
                    };
                    for mut shard in children {
                        // Carry the parent's generation into the child
                        shard.generation = parent.generation;

                        debug_assert!(shard.splitting == SplitState::Splitting);
                        diesel::insert_into(tenant_shards)
                            .values(shard)
                            .execute(conn)?;
                    }
                }

                Ok(())
            })?;

            Ok(())
        })
        .await
    }

    // When we finish shard splitting, we must atomically clean up the old shards
    // and insert the new shards, and clear the splitting marker.
    pub(crate) async fn complete_shard_split(
        &self,
        split_tenant_id: TenantId,
        old_shard_count: ShardCount,
    ) -> DatabaseResult<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_conn(move |conn| -> DatabaseResult<()> {
            conn.transaction(|conn| -> QueryResult<()> {
                // Drop parent shards
                diesel::delete(tenant_shards)
                    .filter(tenant_id.eq(split_tenant_id.to_string()))
                    .filter(shard_count.eq(old_shard_count.literal() as i32))
                    .execute(conn)?;

                // Clear sharding flag
                let updated = diesel::update(tenant_shards)
                    .filter(tenant_id.eq(split_tenant_id.to_string()))
                    .set((splitting.eq(0),))
                    .execute(conn)?;
                debug_assert!(updated > 0);

                Ok(())
            })?;

            Ok(())
        })
        .await
    }
}

/// Parts of [`crate::tenant_state::TenantState`] that are stored durably
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Clone, Eq, PartialEq)]
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
    pub(crate) splitting: SplitState,
    #[serde(default)]
    pub(crate) config: String,
}

/// Parts of [`crate::node::Node`] that are stored durably
#[derive(Serialize, Deserialize, Queryable, Selectable, Insertable, Eq, PartialEq)]
#[diesel(table_name = crate::schema::nodes)]
pub(crate) struct NodePersistence {
    pub(crate) node_id: i64,
    pub(crate) scheduling_policy: String,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
}
