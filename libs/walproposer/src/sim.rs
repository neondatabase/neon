use std::cell::RefCell;

use safekeeper::simlib::node_os::NodeOs;

thread_local! {
    pub static CURRENT_NODE_OS: RefCell<Option<NodeOs>> = RefCell::new(None);
}

/// Get the current node os.
fn os() -> NodeOs {
    CURRENT_NODE_OS.with(|cell| {
        cell.borrow().clone().expect("no node os set")
    })
}

/// Should be called before calling any of the C functions.
pub fn c_attach_node_os(os: NodeOs) {
    CURRENT_NODE_OS.with(|cell| {
        *cell.borrow_mut() = Some(os);
    });
}

#[no_mangle]
pub extern "C" fn sim_sleep(ms: u64) {
    println!("got a call to sleep for {} ms", ms);
    os().sleep(ms);
}
