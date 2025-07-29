#![allow(dead_code, unused)]

use std::collections::{HashMap, HashSet};

use diesel::Queryable;
use diesel::dsl::min;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use itertools::Itertools;
use pageserver_api::controller_api::SCSafekeeperTimelinesResponse;
use scoped_futures::ScopedFutureExt;
use serde::{Deserialize, Serialize};
use utils::id::{NodeId, TenantId, TimelineId};
use uuid::Uuid;

use crate::hadron_dns::NodeType;
use crate::hadron_requests::NodeConnectionInfo;
use crate::persistence::{DatabaseError, DatabaseResult};
use crate::schema::{hadron_safekeepers, nodes};
use crate::sk_node::SafeKeeperNode;
use std::str::FromStr;

// The Safe Keeper node database representation (for Diesel).
#[derive(
    Clone, Serialize, Deserialize, Queryable, Selectable, Insertable, Eq, PartialEq, AsChangeset,
)]
#[diesel(table_name = crate::schema::hadron_safekeepers)]
pub(crate) struct HadronSafekeeperRow {
    pub(crate) sk_node_id: i64,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
}

#[derive(
    Clone, Serialize, Deserialize, Queryable, Selectable, Insertable, Eq, PartialEq, AsChangeset,
)]
#[diesel(table_name = crate::schema::hadron_timeline_safekeepers)]
pub(crate) struct HadronTimelineSafekeeper {
    pub(crate) timeline_id: String,
    pub(crate) sk_node_id: i64,
    pub(crate) legacy_endpoint_id: Option<Uuid>,
}

pub async fn execute_sk_upsert(
    conn: &mut AsyncPgConnection,
    sk_row: HadronSafekeeperRow,
) -> DatabaseResult<()> {
    // SQL:
    // INSERT INTO hadron_safekeepers (sk_node_id, listen_http_addr, listen_http_port, listen_pg_addr, listen_pg_port)
    // VALUES ($1, $2, $3, $4, $5)
    // ON CONFLICT (sk_node_id)
    // DO UPDATE SET listen_http_addr = $2, listen_http_port = $3, listen_pg_addr = $4, listen_pg_port = $5;

    use crate::schema::hadron_safekeepers::dsl::*;

    diesel::insert_into(hadron_safekeepers)
        .values(&sk_row)
        .on_conflict(sk_node_id)
        .do_update()
        .set(&sk_row)
        .execute(conn)
        .await?;

    Ok(())
}

// Load all safekeeper nodes and their associated timelines from the meta PG. This query is supposed
// to run only once on HCC startup and is used to construct the SafeKeeperScheduler state. Performs
// scans of the hadron_safekeepers and hadron_timeline_safekeepers tables.
pub async fn scan_safekeepers_and_scheduled_timelines(
    conn: &mut AsyncPgConnection,
) -> DatabaseResult<HashMap<NodeId, SafeKeeperNode>> {
    use crate::schema::hadron_safekeepers;
    use crate::schema::hadron_timeline_safekeepers;

    // We first scan the hadron_safekeepers table to constuct the SafeKeeperNode objects. We don't know anything about
    // the timelines scheduled to the safekeepers after this step. We then scan the hadron_timeline_safekeepers table
    // to populate the data structures in the SafeKeeperNode objects to reflect the timelines scheduled to the safekeepers.
    let mut results: HashMap<NodeId, SafeKeeperNode> = hadron_safekeepers::table
        .select((
            hadron_safekeepers::sk_node_id,
            hadron_safekeepers::listen_http_addr,
            hadron_safekeepers::listen_http_port,
            hadron_safekeepers::listen_pg_addr,
            hadron_safekeepers::listen_pg_port,
        ))
        .load::<HadronSafekeeperRow>(conn)
        .await?
        .into_iter()
        .map(|row| {
            let sk_node = SafeKeeperNode {
                id: NodeId(row.sk_node_id as u64),
                listen_http_addr: row.listen_http_addr.clone(),
                listen_http_port: row.listen_http_port as u16,
                listen_pg_addr: row.listen_pg_addr.clone(),
                listen_pg_port: row.listen_pg_port as u16,
                legacy_endpoints: HashMap::new(),
                timelines: HashSet::new(),
            };
            (sk_node.id, sk_node)
        })
        .collect();

    let timeline_sk_rows = hadron_timeline_safekeepers::table
        .select((
            hadron_timeline_safekeepers::sk_node_id,
            hadron_timeline_safekeepers::timeline_id,
            hadron_timeline_safekeepers::legacy_endpoint_id,
        ))
        .load::<(i64, String, Option<Uuid>)>(conn)
        .await?;
    for (sk_node_id, timeline_id, legacy_endpoint_id) in timeline_sk_rows {
        if let Some(sk_node) = results.get_mut(&NodeId(sk_node_id as u64)) {
            let parsed_timeline_id =
                TimelineId::from_str(&timeline_id).map_err(|e: hex::FromHexError| {
                    DatabaseError::Logical(format!("Failed to parse timeline IDs: {e}"))
                })?;
            sk_node.timelines.insert(parsed_timeline_id);
            if let Some(legacy_endpoint_id) = legacy_endpoint_id {
                sk_node
                    .legacy_endpoints
                    .insert(legacy_endpoint_id, parsed_timeline_id);
            }
        }
    }

    Ok(results)
}

// Queries the hadron_timeline_safekeepers table to get the safekeepers assigned to the passed
// timeline. If none are found, persists the input proposed safekeepers to the table and returns
// them.
pub async fn idempotently_persist_or_get_existing_timeline_safekeepers(
    conn: &mut AsyncPgConnection,
    timeline_id: TimelineId,
    safekeepers: &[NodeId],
) -> DatabaseResult<Vec<NodeId>> {
    use crate::schema::hadron_timeline_safekeepers;
    // Confirm and persist the timeline-safekeeper mapping. If there are existing safekeepers
    // assigned to the timeline in the database, treat those as the source of truth.
    let existing_safekeepers: Vec<i64> = hadron_timeline_safekeepers::table
        .select(hadron_timeline_safekeepers::sk_node_id)
        .filter(hadron_timeline_safekeepers::timeline_id.eq(timeline_id.to_string()))
        .load::<i64>(conn)
        .await?;
    let confirmed_safekeepers: Vec<NodeId> = if existing_safekeepers.is_empty() {
        let proposed_safekeeper_endpoint_rows_result: Result<Vec<HadronTimelineSafekeeper>, _> =
            safekeepers
                .iter()
                .map(|sk_node_id| {
                    i64::try_from(sk_node_id.0).map(|sk_node_id| HadronTimelineSafekeeper {
                        timeline_id: timeline_id.to_string(),
                        sk_node_id,
                        legacy_endpoint_id: None,
                    })
                })
                .collect();

        let proposed_safekeeper_endpoint_rows =
            proposed_safekeeper_endpoint_rows_result.map_err(|e| {
                DatabaseError::Logical(format!("Failed to convert safekeeper IDs: {e}"))
            })?;

        diesel::insert_into(hadron_timeline_safekeepers::table)
            .values(&proposed_safekeeper_endpoint_rows)
            .execute(conn)
            .await?;
        safekeepers.to_owned()
    } else {
        let safekeeper_result: Result<Vec<NodeId>, _> = existing_safekeepers
            .into_iter()
            .map(|arg0: i64| u64::try_from(arg0).map(NodeId))
            .collect();

        safekeeper_result
            .map_err(|e| DatabaseError::Logical(format!("Failed to convert safekeeper IDs: {e}")))?
    };

    Ok(confirmed_safekeepers)
}

pub async fn delete_timeline_safekeepers(
    conn: &mut AsyncPgConnection,
    timeline_id: TimelineId,
) -> DatabaseResult<()> {
    use crate::schema::hadron_timeline_safekeepers;

    diesel::delete(hadron_timeline_safekeepers::table)
        .filter(hadron_timeline_safekeepers::timeline_id.eq(timeline_id.to_string()))
        .execute(conn)
        .await?;

    Ok(())
}

pub(crate) async fn execute_safekeeper_list_timelines(
    conn: &mut AsyncPgConnection,
    safekeeper_id: i64,
) -> DatabaseResult<SCSafekeeperTimelinesResponse> {
    use crate::schema::hadron_timeline_safekeepers;
    use pageserver_api::controller_api::SCSafekeeperTimelinesResponse;

    conn.transaction(|conn| {
        async move {
            let mut sk_timelines = SCSafekeeperTimelinesResponse {
                timelines: Vec::new(),
                safekeeper_peers: Vec::new(),
            };

            // Find all timelines <String>
            let timeline_ids = hadron_timeline_safekeepers::table
                .select(hadron_timeline_safekeepers::timeline_id)
                .filter(hadron_timeline_safekeepers::sk_node_id.eq(safekeeper_id))
                .load::<String>(conn)
                .await
                .into_iter()
                .flatten()
                .collect_vec();

            // Find the peers for each timeline. <timeline_id, sk_node_id>
            let timeline_peers = hadron_timeline_safekeepers::table
                .select((
                    hadron_timeline_safekeepers::timeline_id,
                    hadron_timeline_safekeepers::sk_node_id,
                ))
                .filter(hadron_timeline_safekeepers::timeline_id.eq_any(&timeline_ids))
                .load::<(String, i64)>(conn)
                .await
                .into_iter()
                .flatten()
                .collect_vec();

            let mut timeline_peers_map = HashMap::new();
            let mut seen = HashSet::new();
            let mut unique_sks = Vec::new();

            for (timeline_id, sk_node_id) in timeline_peers {
                timeline_peers_map
                    .entry(timeline_id)
                    .or_insert_with(Vec::new)
                    .push(sk_node_id);
                if seen.insert(sk_node_id) {
                    unique_sks.push(sk_node_id);
                }
            }

            // Find SK info.
            let mut found_sk_nodes = HashSet::new();
            hadron_safekeepers::table
                .select((
                    hadron_safekeepers::sk_node_id,
                    hadron_safekeepers::listen_http_addr,
                    hadron_safekeepers::listen_http_port,
                ))
                .filter(hadron_safekeepers::sk_node_id.eq_any(&unique_sks))
                .load::<(i64, String, i32)>(conn)
                .await
                .into_iter()
                .flatten()
                .for_each(|(sk_node_id, listen_http_addr, http_port)| {
                    found_sk_nodes.insert(sk_node_id);

                    sk_timelines.safekeeper_peers.push(
                        pageserver_api::controller_api::TimelineSafekeeperPeer {
                            node_id: utils::id::NodeId(sk_node_id as u64),
                            listen_http_addr,
                            http_port,
                        },
                    );
                });

            // Prepare timeline response.
            for timeline_id in timeline_ids {
                if !timeline_peers_map.contains_key(&timeline_id) {
                    continue;
                }
                let peers = timeline_peers_map.get(&timeline_id).unwrap();
                // Check peers exist.
                if !peers
                    .iter()
                    .all(|sk_node_id| found_sk_nodes.contains(sk_node_id))
                {
                    continue;
                }

                let timeline = pageserver_api::controller_api::SCSafekeeperTimeline {
                    timeline_id: TimelineId::from_str(&timeline_id).unwrap(),
                    peers: peers
                        .iter()
                        .map(|sk_node_id| utils::id::NodeId(*sk_node_id as u64))
                        .collect(),
                };
                sk_timelines.timelines.push(timeline);
            }

            Ok(sk_timelines)
        }
        .scope_boxed()
    })
    .await
}

/// Stores details about connecting to pageserver and safekeeper nodes for a given tenant and
/// timeline.
pub struct PageserverAndSafekeeperConnectionInfo {
    pub pageserver_conn_info: Vec<NodeConnectionInfo>,
    pub safekeeper_conn_info: Vec<NodeConnectionInfo>,
}

/// Retrieves the connection information for the pageserver and safekeepers associated with the
/// given tenant and timeline.
pub async fn get_pageserver_and_safekeeper_connection_info(
    conn: &mut AsyncPgConnection,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> DatabaseResult<PageserverAndSafekeeperConnectionInfo> {
    conn.transaction(|conn| {
        async move {
            // Fetch details about pageserver, which is associated with the input tenant.
            let pageserver_conn_info =
                get_pageserver_connection_info(conn, &tenant_id.to_string()).await?;

            // Fetch details about safekeepers, which are associated with the input timeline.
            let safekeeper_conn_info =
                get_safekeeper_connection_info(conn, &timeline_id.to_string()).await?;

            Ok(PageserverAndSafekeeperConnectionInfo {
                pageserver_conn_info,
                safekeeper_conn_info,
            })
        }
        .scope_boxed()
    })
    .await
}

async fn get_safekeeper_connection_info(
    conn: &mut AsyncPgConnection,
    timeline_id: &str,
) -> DatabaseResult<Vec<NodeConnectionInfo>> {
    use crate::schema::hadron_safekeepers;
    use crate::schema::hadron_timeline_safekeepers;

    Ok(hadron_timeline_safekeepers::table
        .inner_join(
            hadron_safekeepers::table
                .on(hadron_timeline_safekeepers::sk_node_id.eq(hadron_safekeepers::sk_node_id)),
        )
        .select((
            hadron_safekeepers::sk_node_id,
            hadron_safekeepers::listen_pg_addr,
            hadron_safekeepers::listen_pg_port,
        ))
        .filter(hadron_timeline_safekeepers::timeline_id.eq(timeline_id.to_string()))
        .load::<(i64, String, i32)>(conn)
        .await?
        .into_iter()
        .map(|(node_id, addr, port)| {
            NodeConnectionInfo::new(
                NodeType::Safekeeper,
                NodeId(node_id as u64),
                addr,
                port as u16,
            )
        })
        .collect())
}

async fn get_pageserver_connection_info(
    conn: &mut AsyncPgConnection,
    tenant_id: &str,
) -> DatabaseResult<Vec<NodeConnectionInfo>> {
    use crate::schema::tenant_shards;

    // When the tenant is being split, it'll contain both old shards and new shards. Until the tenant split is committed,
    // we should always use the old shards.
    // NOTE: we only support tenant split without tennat merge. Thus shard count could only increase.
    let min_shard_count = match tenant_shards::table
        .select(min(tenant_shards::shard_count))
        .filter(tenant_shards::tenant_id.eq(tenant_id))
        .first::<Option<i32>>(conn)
        .await
        .optional()?
    {
        Some(Some(count)) => count,
        Some(None) => {
            // Tenant doesn't exist. It's possible that it was deleted before we got the request.
            return Ok(vec![]);
        }
        None => {
            // This is never supposed to happen because `SELECT min()` should always return one row.
            return Err(DatabaseError::Logical(format!(
                "Unexpected empty query result for min(shard_count) query. Tenant ID {tenant_id}"
            )));
        }
    };

    let shards: Vec<NodeConnectionInfo> = nodes::table
        .inner_join(
            tenant_shards::table.on(nodes::node_id
                .nullable()
                .eq(tenant_shards::generation_pageserver)),
        )
        .select((nodes::node_id, nodes::listen_pg_addr, nodes::listen_pg_port))
        .filter(tenant_shards::tenant_id.eq(&tenant_id.to_string()))
        .order(tenant_shards::shard_number.asc())
        .filter(tenant_shards::shard_count.eq(min_shard_count))
        .load::<(i64, String, i32)>(conn)
        .await?
        .into_iter()
        .map(|(node_id, addr, port)| {
            NodeConnectionInfo::new(
                NodeType::Pageserver,
                NodeId(node_id as u64),
                addr,
                port as u16,
            )
        })
        .collect();

    if !shards.is_empty() && !shards.len().is_power_of_two() {
        return Err(DatabaseError::Logical(format!(
            "Tenant {} has unexpected shard count {} (not a power of 2)",
            tenant_id,
            shards.len()
        )));
    }
    Ok(shards)
}
