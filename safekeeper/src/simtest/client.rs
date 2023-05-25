use crate::simlib::{
    node_os::NodeOs,
    proto::{AnyMessage, ReplCell},
    world::{NodeEvent, NodeId},
};

/// Copy all data from array to the remote node.
pub fn run_client(os: NodeOs, data: &[ReplCell], dst: NodeId) {
    println!("started client");

    let epoll = os.epoll();
    let mut delivered = 0;

    let mut sock = os.open_tcp(dst);

    while delivered < data.len() {
        let num = &data[delivered];
        println!("sending data: {:?}", num.clone());
        sock.send(AnyMessage::ReplCell(num.clone()));

        // loop {
        let event = epoll.recv();
        match event {
            NodeEvent::Message((AnyMessage::Just32(flush_pos), _)) => {
                if flush_pos == 1 + delivered as u32 {
                    delivered += 1;
                }
            }
            NodeEvent::Closed(_) => {
                println!("connection closed, reestablishing");
                sock = os.open_tcp(dst);
            }
            _ => {}
        }

        // }
    }

    let sock = os.open_tcp(dst);
    for num in data {
        println!("sending data: {:?}", num.clone());
        sock.send(AnyMessage::ReplCell(num.clone()));
    }

    println!("sent all data and finished client");
}
