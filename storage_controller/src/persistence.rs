pub(crate) mod split_state;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;

use self::split_state::SplitState;
use anyhow::bail;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::Connection;
use pageserver_api::controller_api::MetadataHealthRecord;
use pageserver_api::controller_api::ShardSchedulingPolicy;
use pageserver_api::controller_api::{NodeSchedulingPolicy, PlacementPolicy};
use pageserver_api::models::TenantConfig;
use pageserver_api::shard::ShardConfigError;
use pageserver_api::shard::ShardIdentity;
use pageserver_api::shard::ShardStripeSize;
use pageserver_api::shard::{ShardCount, ShardNumber, TenantShardId};
use serde::{Deserialize, Serialize};
use utils::generation::Generation;
use utils::id::{NodeId, TenantId};

use crate::metrics::{
    DatabaseQueryErrorLabelGroup, DatabaseQueryLatencyLabelGroup, METRICS_REGISTRY,
};
use crate::node::Node;

/// ## What do we store?
///
/// The storage controller service does not store most of its state durably.
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
/// The storage controller service does not go via the database for most things: there are
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

#[derive(measured::FixedCardinalityLabel, Copy, Clone)]
pub(crate) enum DatabaseOperation {
    InsertNode,
    UpdateNode,
    DeleteNode,
    ListNodes,
    BeginShardSplit,
    CompleteShardSplit,
    AbortShardSplit,
    Detach,
    ReAttach,
    IncrementGeneration,
    ListTenantShards,
    InsertTenantShards,
    UpdateTenantShard,
    DeleteTenant,
    UpdateTenantConfig,
    UpdateMetadataHealth,
    ListMetadataHealth,
    ListMetadataHealthUnhealthy,
    ListMetadataHealthOutdated,
}

#[must_use]
pub(crate) enum AbortShardSplitStatus {
    /// We aborted the split in the database by reverting to the parent shards
    Aborted,
    /// The split had already been persisted.
    Complete,
}

pub(crate) type DatabaseResult<T> = Result<T, DatabaseError>;

/// Some methods can operate on either a whole tenant or a single shard
pub(crate) enum TenantFilter {
    Tenant(TenantId),
    Shard(TenantShardId),
}

impl Persistence {
    // The default postgres connection limit is 100.  We use up to 99, to leave one free for a human admin under
    // normal circumstances.  This assumes we have exclusive use of the database cluster to which we connect.
    pub const MAX_CONNECTIONS: u32 = 99;

    // We don't want to keep a lot of connections alive: close them down promptly if they aren't being used.
    const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(60);

    pub fn new(database_url: String) -> Self {
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

        Self { connection_pool }
    }

    /// A helper for use during startup, where we would like to tolerate concurrent restarts of the
    /// database and the storage controller, therefore the database might not be available right away
    pub async fn await_connection(
        database_url: &str,
        timeout: Duration,
    ) -> Result<(), diesel::ConnectionError> {
        let started_at = Instant::now();
        loop {
            match PgConnection::establish(database_url) {
                Ok(_) => {
                    tracing::info!("Connected to database.");
                    return Ok(());
                }
                Err(e) => {
                    if started_at.elapsed() > timeout {
                        return Err(e);
                    } else {
                        tracing::info!("Database not yet available, waiting... ({e})");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    /// Wraps `with_conn` in order to collect latency and error metrics
    async fn with_measured_conn<F, R>(&self, op: DatabaseOperation, func: F) -> DatabaseResult<R>
    where
        F: Fn(&mut PgConnection) -> DatabaseResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let latency = &METRICS_REGISTRY
            .metrics_group
            .storage_controller_database_query_latency;
        let _timer = latency.start_timer(DatabaseQueryLatencyLabelGroup { operation: op });

        let res = self.with_conn(func).await;

        if let Err(err) = &res {
            let error_counter = &METRICS_REGISTRY
                .metrics_group
                .storage_controller_database_query_error;
            error_counter.inc(DatabaseQueryErrorLabelGroup {
                error_type: err.error_label(),
                operation: op,
            })
        }

        res
    }

    /// Call the provided function in a tokio blocking thread, with a Diesel database connection.
    async fn with_conn<F, R>(&self, func: F) -> DatabaseResult<R>
    where
        F: Fn(&mut PgConnection) -> DatabaseResult<R> + Send + 'static,
        R: Send + 'static,
    {
        // A generous allowance for how many times we may retry serializable transactions
        // before giving up.  This is not expected to be hit: it is a defensive measure in case we
        // somehow engineer a situation where duelling transactions might otherwise live-lock.
        const MAX_RETRIES: usize = 128;

        let mut conn = self.connection_pool.get()?;
        tokio::task::spawn_blocking(move || -> DatabaseResult<R> {
            let mut retry_count = 0;
            loop {
                match conn.build_transaction().serializable().run(|c| func(c)) {
                    Ok(r) => break Ok(r),
                    Err(
                        err @ DatabaseError::Query(diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::SerializationFailure,
                            _,
                        )),
                    ) => {
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            tracing::error!(
                                "Exceeded max retries on SerializationFailure errors: {err:?}"
                            );
                            break Err(err);
                        } else {
                            // Retry on serialization errors: these are expected, because even though our
                            // transactions don't fight for the same rows, they will occasionally collide
                            // on index pages (e.g. increment_generation for unrelated shards can collide)
                            tracing::debug!(
                                "Retrying transaction on serialization failure {err:?}"
                            );
                            continue;
                        }
                    }
                    Err(e) => break Err(e),
                }
            }
        })
        .await
        .expect("Task panic")
    }

    /// When a node is first registered, persist it before using it for anything
    pub(crate) async fn insert_node(&self, node: &Node) -> DatabaseResult<()> {
        let np = node.to_persistent();
        self.with_measured_conn(
            DatabaseOperation::InsertNode,
            move |conn| -> DatabaseResult<()> {
                diesel::insert_into(crate::schema::nodes::table)
                    .values(&np)
                    .execute(conn)?;
                Ok(())
            },
        )
        .await
    }

    /// At startup, populate the list of nodes which our shards may be placed on
    pub(crate) async fn list_nodes(&self) -> DatabaseResult<Vec<NodePersistence>> {
        let nodes: Vec<NodePersistence> = self
            .with_measured_conn(
                DatabaseOperation::ListNodes,
                move |conn| -> DatabaseResult<_> {
                    Ok(crate::schema::nodes::table.load::<NodePersistence>(conn)?)
                },
            )
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
            .with_measured_conn(DatabaseOperation::UpdateNode, move |conn| {
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
        self.with_measured_conn(
            DatabaseOperation::ListTenantShards,
            move |conn| -> DatabaseResult<_> {
                Ok(crate::schema::tenant_shards::table.load::<TenantShardPersistence>(conn)?)
            },
        )
        .await
    }

    /// Tenants must be persisted before we schedule them for the first time.  This enables us
    /// to correctly retain generation monotonicity, and the externally provided placement policy & config.
    pub(crate) async fn insert_tenant_shards(
        &self,
        shards: Vec<TenantShardPersistence>,
    ) -> DatabaseResult<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_measured_conn(
            DatabaseOperation::InsertTenantShards,
            move |conn| -> DatabaseResult<()> {
                for tenant in &shards {
                    diesel::insert_into(tenant_shards)
                        .values(tenant)
                        .execute(conn)?;
                }
                Ok(())
            },
        )
        .await
    }

    /// Ordering: call this _after_ deleting the tenant on pageservers, but _before_ dropping state for
    /// the tenant from memory on this server.
    pub(crate) async fn delete_tenant(&self, del_tenant_id: TenantId) -> DatabaseResult<()> {
        use crate::schema::metadata_health;
        use crate::schema::tenant_shards;
        self.with_measured_conn(
            DatabaseOperation::DeleteTenant,
            move |conn| -> DatabaseResult<()> {
                diesel::delete(tenant_shards::table)
                    .filter(tenant_shards::tenant_id.eq(del_tenant_id.to_string()))
                    .execute(conn)?;

                // Delete metadata health status as well
                diesel::delete(metadata_health::table)
                    .filter(metadata_health::tenant_id.eq(del_tenant_id.to_string()))
                    .execute(conn)?;

                Ok(())
            },
        )
        .await
    }

    pub(crate) async fn delete_node(&self, del_node_id: NodeId) -> DatabaseResult<()> {
        use crate::schema::nodes::dsl::*;
        self.with_measured_conn(
            DatabaseOperation::DeleteNode,
            move |conn| -> DatabaseResult<()> {
                diesel::delete(nodes)
                    .filter(node_id.eq(del_node_id.0 as i64))
                    .execute(conn)?;

                Ok(())
            },
        )
        .await
    }

    /// When a tenant invokes the /re-attach API, this function is responsible for doing an efficient
    /// batched increment of the generations of all tenants whose generation_pageserver is equal to
    /// the node that called /re-attach.
    #[tracing::instrument(skip_all, fields(node_id))]
    pub(crate) async fn re_attach(
        &self,
        input_node_id: NodeId,
    ) -> DatabaseResult<HashMap<TenantShardId, Generation>> {
        use crate::schema::nodes::dsl::scheduling_policy;
        use crate::schema::nodes::dsl::*;
        use crate::schema::tenant_shards::dsl::*;
        let updated = self
            .with_measured_conn(DatabaseOperation::ReAttach, move |conn| {
                let rows_updated = diesel::update(tenant_shards)
                    .filter(generation_pageserver.eq(input_node_id.0 as i64))
                    .set(generation.eq(generation + 1))
                    .execute(conn)?;

                tracing::info!("Incremented {} tenants' generations", rows_updated);

                // TODO: UPDATE+SELECT in one query

                let updated = tenant_shards
                    .filter(generation_pageserver.eq(input_node_id.0 as i64))
                    .select(TenantShardPersistence::as_select())
                    .load(conn)?;

                // If the node went through a drain and restart phase before re-attaching,
                // then reset it's node scheduling policy to active.
                diesel::update(nodes)
                    .filter(node_id.eq(input_node_id.0 as i64))
                    .filter(
                        scheduling_policy
                            .eq(String::from(NodeSchedulingPolicy::PauseForRestart))
                            .or(scheduling_policy.eq(String::from(NodeSchedulingPolicy::Draining)))
                            .or(scheduling_policy.eq(String::from(NodeSchedulingPolicy::Filling))),
                    )
                    .set(scheduling_policy.eq(String::from(NodeSchedulingPolicy::Active)))
                    .execute(conn)?;

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

            let Some(g) = tsp.generation else {
                // If the generation_pageserver column was non-NULL, then the generation column should also be non-NULL:
                // we only set generation_pageserver when setting generation.
                return Err(DatabaseError::Logical(
                    "Generation should always be set after incrementing".to_string(),
                ));
            };
            result.insert(tenant_shard_id, Generation::new(g as u32));
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
            .with_measured_conn(DatabaseOperation::IncrementGeneration, move |conn| {
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

        // Generation is always non-null in the rseult: if the generation column had been NULL, then we
        // should have experienced an SQL Confilict error while executing a query that tries to increment it.
        debug_assert!(updated.generation.is_some());
        let Some(g) = updated.generation else {
            return Err(DatabaseError::Logical(
                "Generation should always be set after incrementing".to_string(),
            )
            .into());
        };

        Ok(Generation::new(g as u32))
    }

    #[allow(non_local_definitions)]
    /// For use when updating a persistent property of a tenant, such as its config or placement_policy.
    ///
    /// Do not use this for settting generation, unless in the special onboarding code path (/location_config)
    /// API: use [`Self::increment_generation`] instead.  Setting the generation via this route is a one-time thing
    /// that we only do the first time a tenant is set to an attached policy via /location_config.
    pub(crate) async fn update_tenant_shard(
        &self,
        tenant: TenantFilter,
        input_placement_policy: Option<PlacementPolicy>,
        input_config: Option<TenantConfig>,
        input_generation: Option<Generation>,
        input_scheduling_policy: Option<ShardSchedulingPolicy>,
    ) -> DatabaseResult<()> {
        use crate::schema::tenant_shards::dsl::*;

        self.with_measured_conn(DatabaseOperation::UpdateTenantShard, move |conn| {
            let query = match tenant {
                TenantFilter::Shard(tenant_shard_id) => diesel::update(tenant_shards)
                    .filter(tenant_id.eq(tenant_shard_id.tenant_id.to_string()))
                    .filter(shard_number.eq(tenant_shard_id.shard_number.0 as i32))
                    .filter(shard_count.eq(tenant_shard_id.shard_count.literal() as i32))
                    .into_boxed(),
                TenantFilter::Tenant(input_tenant_id) => diesel::update(tenant_shards)
                    .filter(tenant_id.eq(input_tenant_id.to_string()))
                    .into_boxed(),
            };

            #[derive(AsChangeset)]
            #[diesel(table_name = crate::schema::tenant_shards)]
            struct ShardUpdate {
                generation: Option<i32>,
                placement_policy: Option<String>,
                config: Option<String>,
                scheduling_policy: Option<String>,
            }

            let update = ShardUpdate {
                generation: input_generation.map(|g| g.into().unwrap() as i32),
                placement_policy: input_placement_policy
                    .as_ref()
                    .map(|p| serde_json::to_string(&p).unwrap()),
                config: input_config
                    .as_ref()
                    .map(|c| serde_json::to_string(&c).unwrap()),
                scheduling_policy: input_scheduling_policy
                    .map(|p| serde_json::to_string(&p).unwrap()),
            };

            query.set(update).execute(conn)?;

            Ok(())
        })
        .await?;

        Ok(())
    }

    pub(crate) async fn detach(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_measured_conn(DatabaseOperation::Detach, move |conn| {
            let updated = diesel::update(tenant_shards)
                .filter(tenant_id.eq(tenant_shard_id.tenant_id.to_string()))
                .filter(shard_number.eq(tenant_shard_id.shard_number.0 as i32))
                .filter(shard_count.eq(tenant_shard_id.shard_count.literal() as i32))
                .set((
                    generation_pageserver.eq(Option::<i64>::None),
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
        self.with_measured_conn(DatabaseOperation::BeginShardSplit, move |conn| -> DatabaseResult<()> {
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
        self.with_measured_conn(
            DatabaseOperation::CompleteShardSplit,
            move |conn| -> DatabaseResult<()> {
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
            },
        )
        .await
    }

    /// Used when the remote part of a shard split failed: we will revert the database state to have only
    /// the parent shards, with SplitState::Idle.
    pub(crate) async fn abort_shard_split(
        &self,
        split_tenant_id: TenantId,
        new_shard_count: ShardCount,
    ) -> DatabaseResult<AbortShardSplitStatus> {
        use crate::schema::tenant_shards::dsl::*;
        self.with_measured_conn(
            DatabaseOperation::AbortShardSplit,
            move |conn| -> DatabaseResult<AbortShardSplitStatus> {
                // Clear the splitting state on parent shards
                let updated = diesel::update(tenant_shards)
                    .filter(tenant_id.eq(split_tenant_id.to_string()))
                    .filter(shard_count.ne(new_shard_count.literal() as i32))
                    .set((splitting.eq(0),))
                    .execute(conn)?;

                // Parent shards are already gone: we cannot abort.
                if updated == 0 {
                    return Ok(AbortShardSplitStatus::Complete);
                }

                // Sanity check: if parent shards were present, their cardinality should
                // be less than the number of child shards.
                if updated >= new_shard_count.count() as usize {
                    return Err(DatabaseError::Logical(format!(
                        "Unexpected parent shard count {updated} while aborting split to \
                            count {new_shard_count:?} on tenant {split_tenant_id}"
                    )));
                }

                // Erase child shards
                diesel::delete(tenant_shards)
                    .filter(tenant_id.eq(split_tenant_id.to_string()))
                    .filter(shard_count.eq(new_shard_count.literal() as i32))
                    .execute(conn)?;

                Ok(AbortShardSplitStatus::Aborted)
            },
        )
        .await
    }

    /// Stores all the latest metadata health updates durably. Updates existing entry on conflict.
    ///
    /// **Correctness:** `metadata_health_updates` should all belong the tenant shards managed by the storage controller.
    #[allow(dead_code)]
    pub(crate) async fn update_metadata_health_records(
        &self,
        metadata_health_records: Vec<MetadataHealthPersistence>,
    ) -> DatabaseResult<()> {
        use crate::schema::metadata_health::dsl::*;

        self.with_measured_conn(
            DatabaseOperation::UpdateMetadataHealth,
            move |conn| -> DatabaseResult<_> {
                for update in &metadata_health_records {
                    diesel::insert_into(metadata_health)
                        .values(update)
                        .on_conflict((tenant_id, shard_number, shard_count))
                        .do_update()
                        .set((
                            healthy.eq(update.healthy),
                            last_scrubbed_at.eq(update.last_scrubbed_at),
                        ))
                        .execute(conn)?;
                }
                Ok(())
            },
        )
        .await
    }

    /// Lists all the metadata health records.
    #[allow(dead_code)]
    pub(crate) async fn list_metadata_health_records(
        &self,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        self.with_measured_conn(
            DatabaseOperation::ListMetadataHealth,
            move |conn| -> DatabaseResult<_> {
                Ok(
                    crate::schema::metadata_health::table
                        .load::<MetadataHealthPersistence>(conn)?,
                )
            },
        )
        .await
    }

    /// Lists all the metadata health records that is unhealthy.
    #[allow(dead_code)]
    pub(crate) async fn list_unhealthy_metadata_health_records(
        &self,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        use crate::schema::metadata_health::dsl::*;
        self.with_measured_conn(
            DatabaseOperation::ListMetadataHealthUnhealthy,
            move |conn| -> DatabaseResult<_> {
                Ok(crate::schema::metadata_health::table
                    .filter(healthy.eq(false))
                    .load::<MetadataHealthPersistence>(conn)?)
            },
        )
        .await
    }

    /// Lists all the metadata health records that have not been updated since an `earlier` time.
    #[allow(dead_code)]
    pub(crate) async fn list_outdated_metadata_health_records(
        &self,
        earlier: chrono::DateTime<chrono::Utc>,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        use crate::schema::metadata_health::dsl::*;
        self.with_measured_conn(
            DatabaseOperation::ListMetadataHealthOutdated,
            move |conn| -> DatabaseResult<_> {
                Ok(crate::schema::metadata_health::table
                    .filter(last_scrubbed_at.lt(earlier))
                    .load::<MetadataHealthPersistence>(conn)?)
            },
        )
        .await
    }
}

/// Parts of [`crate::tenant_shard::TenantShard`] that are stored durably
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
    // and use the incremented number when attaching.
    //
    // Generation is only None when first onboarding a tenant, where it may
    // be in PlacementPolicy::Secondary and therefore have no valid generation state.
    pub(crate) generation: Option<i32>,

    // Currently attached pageserver
    #[serde(rename = "pageserver")]
    pub(crate) generation_pageserver: Option<i64>,

    #[serde(default)]
    pub(crate) placement_policy: String,
    #[serde(default)]
    pub(crate) splitting: SplitState,
    #[serde(default)]
    pub(crate) config: String,
    #[serde(default)]
    pub(crate) scheduling_policy: String,
}

impl TenantShardPersistence {
    pub(crate) fn get_shard_identity(&self) -> Result<ShardIdentity, ShardConfigError> {
        if self.shard_count == 0 {
            Ok(ShardIdentity::unsharded())
        } else {
            Ok(ShardIdentity::new(
                ShardNumber(self.shard_number as u8),
                ShardCount::new(self.shard_count as u8),
                ShardStripeSize(self.shard_stripe_size as u32),
            )?)
        }
    }

    pub(crate) fn get_tenant_shard_id(&self) -> Result<TenantShardId, hex::FromHexError> {
        Ok(TenantShardId {
            tenant_id: TenantId::from_str(self.tenant_id.as_str())?,
            shard_number: ShardNumber(self.shard_number as u8),
            shard_count: ShardCount::new(self.shard_count as u8),
        })
    }
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

/// Tenant metadata health status that are stored durably.
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[diesel(table_name = crate::schema::metadata_health)]
pub(crate) struct MetadataHealthPersistence {
    #[serde(default)]
    pub(crate) tenant_id: String,
    #[serde(default)]
    pub(crate) shard_number: i32,
    #[serde(default)]
    pub(crate) shard_count: i32,

    pub(crate) healthy: bool,
    pub(crate) last_scrubbed_at: chrono::DateTime<chrono::Utc>,
}

impl MetadataHealthPersistence {
    #[allow(dead_code)]
    pub(crate) fn get_tenant_shard_id(&self) -> Result<TenantShardId, hex::FromHexError> {
        Ok(TenantShardId {
            tenant_id: TenantId::from_str(self.tenant_id.as_str())?,
            shard_number: ShardNumber(self.shard_number as u8),
            shard_count: ShardCount::new(self.shard_count as u8),
        })
    }
}

impl TryFrom<MetadataHealthRecord> for MetadataHealthPersistence {
    type Error = anyhow::Error;

    fn try_from(value: MetadataHealthRecord) -> Result<Self, Self::Error> {
        match (value.healthy, value.last_scrubbed_at) {
            (Some(healthy), Some(last_scrubbed_at)) => {
                let tenant_id = value.tenant_shard_id.tenant_id.to_string();
                let shard_number = value.tenant_shard_id.shard_number.0 as i32;
                let shard_count = value.tenant_shard_id.shard_count.literal() as i32;
                Ok(MetadataHealthPersistence {
                    tenant_id,
                    shard_number,
                    shard_count,
                    healthy,
                    last_scrubbed_at,
                })
            }
            _ => bail!("healthy and last_scrubbed_at must not be None."),
        }
    }
}

impl From<MetadataHealthPersistence> for MetadataHealthRecord {
    fn from(value: MetadataHealthPersistence) -> Self {
        MetadataHealthRecord {
            tenant_shard_id: value
                .get_tenant_shard_id()
                .expect("stored tenant id should be valid"),
            healthy: Some(value.healthy),
            last_scrubbed_at: Some(value.last_scrubbed_at),
        }
    }
}
