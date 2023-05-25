use crate::simlib::{node_os::NodeOs, proto::AnyMessage, world::NodeEvent};

use super::disk::Storage;

// pub struct DiskLog {
//     pub map: HashMap<String, u32>,
// }

// impl DiskLog {
//     pub fn new() -> Self {
//         Self {
//             map: HashMap::new(),
//         }
//     }

//     pub fn get(&self, key: &str) -> u32 {
//         self.map.get(key).copied().unwrap_or(0)
//     }

//     pub fn set(&mut self, key: &str, value: u32) {
//         self.map.insert(key.to_string(), value);
//     }
// }

pub fn run_server(os: NodeOs, mut storage: Box<dyn Storage<u32>>) {
    println!("started server");

    let epoll = os.epoll();
    loop {
        let event = epoll.recv();
        println!("got event: {:?}", event);
        match event {
            NodeEvent::Message((msg, tcp)) => match msg {
                AnyMessage::ReplCell(cell) => {
                    if cell.seqno != storage.flush_pos() {
                        println!("got out of order data: {:?}", cell);
                        continue;
                    }
                    storage.write(cell.value);
                    storage.flush().unwrap();
                    tcp.send(AnyMessage::Just32(storage.flush_pos()));
                }
                _ => {}
            },
            NodeEvent::Accept(tcp) => {
                tcp.send(AnyMessage::Just32(storage.flush_pos()));
            }
            _ => {}
        }
    }
}
