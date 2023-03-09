use super::{node_os::NodeOs, world::NodeId, proto::AnyMessage};

/// Copy all data from array to the remote node.
pub fn run_client(os: NodeOs, data: &[u32], dst: NodeId) {
    println!("started client");
    
    let sock = os.open_tcp(dst);
    for num in data {
        sock.send(AnyMessage::Just32(num.clone()));
    }

    println!("sent all data and finished client");
}
