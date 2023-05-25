use crate::simlib::{
    node_os::NodeOs,
    proto::{AnyMessage, ReplCell},
    world::NodeId,
};

/// Copy all data from array to the remote node.
pub fn run_client(os: NodeOs, data: &[ReplCell], dst: NodeId) {
    println!("started client");

    let sock = os.open_tcp(dst);
    for num in data {
        println!("sending data: {:?}", num.clone());
        sock.send(AnyMessage::ReplCell(num.clone()));
    }

    println!("sent all data and finished client");
}
