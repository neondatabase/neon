#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use safekeeper::simlib::node_os::NodeOs;
use tracing::info;

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[no_mangle]
pub extern "C" fn rust_function(a: u32) {
    info!("Hello from Rust!");
    info!("a: {}", a);
}

pub mod sim;
pub mod sim_proto;

#[cfg(test)]
mod test;

#[cfg(test)]
pub mod simtest;

pub fn c_context() -> Option<Box<dyn Fn(NodeOs) + Send + Sync>> {
    Some(Box::new(|os: NodeOs| {
        sim::c_attach_node_os(os);
        unsafe { bindings::MyContextInit(); }
    }))
}

pub fn enable_debug() {
    unsafe { bindings::debug_enabled = true; }
}
