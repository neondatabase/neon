use utils::id::NodeId;

use crate::hadron_dns::NodeType;

/// Internal representation of how a compute node should connect to a PS or SK node. HCC uses this struct to
/// construct connection strings that are passed to the compute node via the compute spec. This struct is never
/// serialized or sent over the wire.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NodeConnectionInfo {
    // Type of the node.
    node_type: NodeType,
    // Node ID. Unique for each node type.
    pub(crate) node_id: NodeId,
    // The hostname reported by the node when it registers. This is the hostname we store in the meta PG, and is
    // typically the k8s cluster DNS name of the node. Note that this may not be resolvable from compute nodes running
    // on dblet. For this reason, this hostname is usually not communicated to the compute node. Instead, HCC computes
    // a DNS name of the node in the Cloud DNS hosted zone based on `node_type` and `node_id` and advertise the DNS name
    // to compute nodes. This hostname here is used as a fallback in tests or other scenarios where we do not have the
    // Cloud DNS hosted zone available.
    registration_hostname: String,
    // The PG wire protocol port on the PS or SK node.
    port: u16,
}

impl NodeConnectionInfo {
    pub(crate) fn new(node_type: NodeType, node_id: NodeId, hostname: String, port: u16) -> Self {
        NodeConnectionInfo {
            node_type,
            node_id,
            registration_hostname: hostname,
            port,
        }
    }
}
