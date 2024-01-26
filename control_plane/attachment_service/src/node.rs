use control_plane::attachment_service::{NodeAvailability, NodeSchedulingPolicy};
use utils::id::NodeId;

use crate::persistence::NodePersistence;

#[derive(Clone)]
pub(crate) struct Node {
    pub(crate) id: NodeId,

    pub(crate) availability: NodeAvailability,
    pub(crate) scheduling: NodeSchedulingPolicy,

    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: u16,

    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: u16,
}

impl Node {
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
    }

    /// Is this node elegible to have work scheduled onto it?
    pub(crate) fn may_schedule(&self) -> bool {
        match self.availability {
            NodeAvailability::Active => {}
            NodeAvailability::Offline => return false,
        }

        match self.scheduling {
            NodeSchedulingPolicy::Active => true,
            NodeSchedulingPolicy::Draining => false,
            NodeSchedulingPolicy::Filling => true,
            NodeSchedulingPolicy::Pause => false,
        }
    }

    pub(crate) fn to_persistent(&self) -> NodePersistence {
        NodePersistence {
            node_id: self.id.0 as i64,
            scheduling_policy: self.scheduling.into(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port as i32,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port as i32,
        }
    }
}
