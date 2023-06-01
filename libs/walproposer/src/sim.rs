use std::{cell::RefCell, collections::HashMap};

use safekeeper::simlib::{network::TCP, node_os::NodeOs, proto::AnyMessage, world::NodeEvent};

use crate::sim_proto::{AnyMessageTag, Event, EventTag, MESSAGE_BUF};

thread_local! {
    static CURRENT_NODE_OS: RefCell<Option<NodeOs>> = RefCell::new(None);
    static TCP_CACHE: RefCell<HashMap<i64, TCP>> = RefCell::new(HashMap::new());
}

/// Get the current node os.
fn os() -> NodeOs {
    CURRENT_NODE_OS.with(|cell| cell.borrow().clone().expect("no node os set"))
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
/// Send MESSAGE_BUF content to the given tcp.
pub extern "C" fn sim_tcp_send(tcp: i64) {
    tcp_load(tcp).send(MESSAGE_BUF.with(|cell| cell.borrow().clone()));
}

#[no_mangle]
pub extern "C" fn sim_epoll_rcv() -> Event {
    let event = os().epoll().recv();
    match event {
        NodeEvent::Accept(tcp) => Event {
            tag: EventTag::Accept,
            tcp: tcp_save(tcp),
            any_message: AnyMessageTag::None,
        },
        NodeEvent::Closed(tcp) => Event {
            tag: EventTag::Closed,
            tcp: tcp_save(tcp),
            any_message: AnyMessageTag::None,
        },
        NodeEvent::Message((message, tcp)) => {
            // store message in thread local storage, C code should use
            // sim_msg_* functions to access it.
            MESSAGE_BUF.with(|cell| {
                *cell.borrow_mut() = message.clone();
            });
            Event {
                tag: EventTag::Message,
                tcp: tcp_save(tcp),
                any_message: match message {
                    AnyMessage::None => AnyMessageTag::None,
                    AnyMessage::InternalConnect => AnyMessageTag::InternalConnect,
                    AnyMessage::Just32(_) => AnyMessageTag::Just32,
                    AnyMessage::ReplCell(_) => AnyMessageTag::ReplCell,
                },
            }
        }
    }
}
