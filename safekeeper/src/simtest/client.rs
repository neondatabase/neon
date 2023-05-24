use crate::simlib::{node_os::NodeOs, proto::{ReplCell, AnyMessage}, world::NodeId};

/// Copy all data from array to the remote node.
pub fn run_client(os: NodeOs, data: &[ReplCell], dst: NodeId) {
    println!("started client");

    let sock = os.open_tcp(dst);
    for num in data {
        println!("start send data from client");
        sock.send(AnyMessage::ReplCell(num.clone()));
        println!("finish send data from client");
    }

    println!("sent all data and finished client");
}
