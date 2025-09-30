use serde::Serialize;
use std::collections::{HashMap, HashSet};
use utils::id::{NodeId, TimelineId};
use uuid::Uuid;

use crate::hadron_queries::HadronSafekeeperRow;

// In-memory representation of a Safe Keeper node.
#[derive(Clone, Serialize)]
pub(crate) struct SafeKeeperNode {
    pub(crate) id: NodeId,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: u16,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: u16,

    // All timelines scheduled to this SK node. Some of the timelines may be associated with
    // a legacy "endpoint", a deprecated concept used in HCC compute CRUD APIs. The "endpoint"
    // concept will be retired after Public Preview launch.
    pub(crate) timelines: HashSet<TimelineId>,
    // All legacy endpoints and their associated timelines scheduled to this SK node.
    // Invariant: The timelines referenced in this map must be present in the `timelines` set above.
    pub(crate) legacy_endpoints: HashMap<Uuid, TimelineId>,
}

impl SafeKeeperNode {
    #[allow(unused)]
    pub(crate) fn new(
        id: NodeId,
        listen_http_addr: String,
        listen_http_port: u16,
        listen_pg_addr: String,
        listen_pg_port: u16,
    ) -> Self {
        Self {
            id,
            listen_http_addr,
            listen_http_port,
            listen_pg_addr,
            listen_pg_port,
            legacy_endpoints: HashMap::new(),
            timelines: HashSet::new(),
        }
    }

    #[allow(unused)]
    pub(crate) fn to_database_row(&self) -> HadronSafekeeperRow {
        HadronSafekeeperRow {
            sk_node_id: self.id.0 as i64,
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port as i32,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port as i32,
        }
    }
}
