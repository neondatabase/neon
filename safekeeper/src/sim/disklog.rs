use std::collections::HashMap;

use super::{node_os::NodeOs, disk::Storage, world::NetworkEvent, proto::AnyMessage};

pub struct DiskLog {
    pub map: HashMap<String, u32>,
}

impl DiskLog {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> u32 {
        self.map.get(key).copied().unwrap_or(0)
    }

    pub fn set(&mut self, key: &str, value: u32) {
        self.map.insert(key.to_string(), value);
    }
}

pub fn run_server(os: NodeOs, mut storage: Box<dyn Storage<u32>>) {
    let epoll = os.network_epoll();
    loop {
        let event = epoll.recv();
        match event {
            NetworkEvent::Message(msg) => {
                match msg {
                    AnyMessage::Just32(num) => {
                        storage.write(num);
                    }
                }
            }
            _ => {},
        }
    }
}
