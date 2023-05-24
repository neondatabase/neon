use crate::simlib::{
    node_os::NodeOs,
    proto::{AnyMessage, ReplCell},
    world::NodeId,
};

/// Copy all data from array to the remote node.
pub fn run_client(os: NodeOs, data: &[ReplCell], dst: NodeId) {
    println!("started client");

    os.sleep(os.random(10000));

    let sock = os.open_tcp(dst);
    for num in data {
        os.sleep(os.random(10000));
        println!("start send data from client");
        sock.send(AnyMessage::ReplCell(num.clone()));
        println!("finish send data from client");
    }

    println!("sent all data and finished client");
}
