use std::{cell::RefCell, collections::HashMap};

use safekeeper::simlib::{node_os::NodeOs, network::TCP, proto::AnyMessage, world::NodeEvent, self};

thread_local! {
    static CURRENT_NODE_OS: RefCell<Option<NodeOs>> = RefCell::new(None);
    static TCP_CACHE: RefCell<HashMap<i64, TCP>> = RefCell::new(HashMap::new());
}

/// Get the current node os.
fn os() -> NodeOs {
    CURRENT_NODE_OS.with(|cell| {
        cell.borrow().clone().expect("no node os set")
    })
}

fn tcp_save(tcp: TCP) -> i64 {
    TCP_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        let id = tcp.id();
        cache.insert(id, tcp);
        id
    })
}

fn tcp_load(id: i64) -> TCP {
    TCP_CACHE.with(|cell| {
        let cache = cell.borrow();
        cache.get(&id).expect("unknown TCP id").clone()
    })
}

/// Should be called before calling any of the C functions.
pub fn c_attach_node_os(os: NodeOs) {
    CURRENT_NODE_OS.with(|cell| {
        *cell.borrow_mut() = Some(os);
    });
    TCP_CACHE.with(|cell| {
        *cell.borrow_mut() = HashMap::new();
    });
}

/// C API for the node os.

#[no_mangle]
pub extern "C" fn sim_sleep(ms: u64) {
    os().sleep(ms);
}

#[no_mangle]
pub extern "C" fn sim_random(max: u64) -> u64 {
    os().random(max)
}

#[no_mangle]
pub extern "C" fn sim_id() -> u32 {
    os().id().into()
}

#[no_mangle]
pub extern "C" fn sim_open_tcp(dst: u32) -> i64 {
    tcp_save(os().open_tcp(dst.into()))
}

#[no_mangle]
// TODO: custom types!!
pub extern "C" fn sim_tcp_send(tcp: i64, value: ReplCell) {
    tcp_load(tcp).send(AnyMessage::ReplCell(simlib::proto::ReplCell {
        value: value.value,
        client_id: value.client_id,
        seqno: value.seqno,
    }));
}

#[no_mangle]
pub extern "C" fn sim_epoll_rcv() -> Event {
    let event = os().epoll().recv();
    match event {
        NodeEvent::Accept(tcp) => Event {
            tcp: tcp_save(tcp),
            value: 0,
            tag: 1,
        },
        NodeEvent::Closed(tcp) => Event {
            tcp: tcp_save(tcp),
            value: 0,
            tag: 2,
        },
        NodeEvent::Message((message, tcp)) => Event {
            tcp: tcp_save(tcp),
            value: match message {
                AnyMessage::Just32(value) => value.into(),
                AnyMessage::ReplCell(cell) => cell.value,
                _ => 0,
            },
            tag: 3,
        },
    }
}

#[repr(C)]
pub struct Event {
    pub tcp: i64,
    // TODO: !!!
    pub value: u32,
    pub tag: u32,
}

#[repr(C)]
pub struct ReplCell {
    pub value: u32,
    pub client_id: u32,
    pub seqno: u32,
}
