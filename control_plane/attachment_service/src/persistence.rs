use std::env;

use anyhow::Context;
use diesel::prelude::*;
use diesel::{Connection, SqliteConnection};
use utils::generation::Generation;

use crate::node::NodeSchedulingPolicy;
use crate::PlacementPolicy;

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
struct Persistence {
    database_url: String,
}

impl Persistence {
    fn new() -> Self {
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        Self { database_url }
    }

    async fn insert_node(&self, node: NodePersistence) -> anyhow::Result<()> {
        let conn = SqliteConnection::establish(&self.database_url).context("Opening database")?;
        diesel::insert_into(crate::schema::nodes::table)
            .values(&node)
            .get_result(&mut conn)
            .into()
    }

    async fn list_nodes(&self) -> anyhow::Result<Vec<NodePersistence>> {
        let mut conn =
            SqliteConnection::establish(&self.database_url).context("Opening database")?;

        crate::schema::nodes::dsl::nodes
            .select(NodePersistence::as_select())
            .load(&mut conn)
            .into()
    }
}

/// Parts of [`crate::tenant_state::TenantState`] that are stored durably
#[derive(Selectable)]
#[diesel(table_name = crate::schema::tenant_shards)]
pub(crate) struct TenantShardPersistence {
    pub(crate) generation: Generation,
    pub(crate) placement_policy: PlacementPolicy,
    pub(crate) config: serde_json::Value,
}

/// Parts of [`crate::node::Node`] that are stored durably
#[derive(Selectable, Insertable)]
#[diesel(table_name = crate::schema::nodes)]
pub(crate) struct NodePersistence {
    pub(crate) node_id: i64,

    pub(crate) scheduling_policy: NodeSchedulingPolicy,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,

    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
}
