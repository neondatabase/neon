//! Functions called from the C code in the worker process

use std::ffi::{CStr, c_char};

use crate::worker_process::main_loop;
use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

/// Launch the communicator's tokio tasks, which do most of the work.
///
/// The caller has initialized the process as a regular PostgreSQL background worker
/// process.
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_process_launch(
    tenant_id: *const c_char,
    timeline_id: *const c_char,
) -> &'static CommunicatorWorkerProcessStruct {
    // Convert the arguments into more convenient Rust types
    let tenant_id = if tenant_id.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(tenant_id) }.to_str().unwrap())
    };
    let timeline_id = if timeline_id.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(timeline_id) }.to_str().unwrap())
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("communicator thread")
        .build()
        .unwrap();

    let worker_struct = runtime.block_on(main_loop::init(
        tenant_id,
        timeline_id,
    ));
    let worker_struct = Box::leak(Box::new(worker_struct));

    runtime.block_on(worker_struct.launch_metrics_exporter());

    // keep the runtime running after we exit this function
    Box::leak(Box::new(runtime));

    worker_struct
}
