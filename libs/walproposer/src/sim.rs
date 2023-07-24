use safekeeper::simlib::{network::TCP, node_os::NodeOs, world::NodeEvent};
use std::{
    cell::RefCell,
    collections::HashMap,
    ffi::{CStr, CString},
};

use crate::sim_proto::{anymessage_tag, AnyMessageTag, Event, EventTag, MESSAGE_BUF};

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
pub(crate) fn c_attach_node_os(os: NodeOs) {
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
pub extern "C" fn sim_epoll_rcv(timeout: i64) -> Event {
    let event = os().epoll_recv(timeout);
    let event = if let Some(event) = event {
        event
    } else {
        return Event {
            tag: EventTag::Timeout,
            tcp: 0,
            any_message: AnyMessageTag::None,
        };
    };

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
                any_message: anymessage_tag(&message),
            }
        }
        NodeEvent::WakeTimeout(_) => {
            // can't happen
            unreachable!()
        }
    }
}

#[no_mangle]
pub extern "C" fn sim_now() -> i64 {
    os().now() as i64
}

#[no_mangle]
pub extern "C" fn sim_exit(code: i32, msg: *const u8) {
    let msg = unsafe { CStr::from_ptr(msg as *const i8) };
    let msg = msg.to_string_lossy().into_owned();
    println!("sim_exit({}, {:?})", code, msg);
    os().set_result(code, msg);

    // I tried to make use of pthread_exit, but it doesn't work.
    // https://github.com/rust-lang/unsafe-code-guidelines/issues/211
    // unsafe { libc::pthread_exit(std::ptr::null_mut()) };

    // https://doc.rust-lang.org/nomicon/unwinding.html
    // Everyone on the internet saying this is UB, but it works for me,
    // so I'm going to use it for now.
    panic!("sim_exit() called from C code")
}
