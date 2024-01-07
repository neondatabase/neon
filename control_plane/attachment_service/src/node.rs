use control_plane::attachment_service::NodeAvailability;
use diesel::expression::AsExpression;
use serde::{Deserialize, Serialize};
use utils::id::NodeId;

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

#[derive(Serialize, Deserialize, Clone, Copy, Debug, AsExpression)]
#[diesel(sql_type = diesel::sql_types::VarChar)]
pub enum NodeSchedulingPolicy {
    // Normal, happy state
    Active,

    // A newly added node: gradually move some work here.
    Filling,

    // Do not schedule new work here, but leave configured locations in place.
    Pause,

    // Do not schedule work here.  Gracefully move work away, as resources allow.
    Draining,
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
}
