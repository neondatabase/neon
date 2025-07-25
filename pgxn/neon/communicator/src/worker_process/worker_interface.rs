//! Functions called from the C code in the worker process

use std::ffi::{CStr, CString, c_char};

use crate::worker_process::main_loop;
use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

/// Launch the communicator's tokio tasks, which do most of the work.
///
/// The caller has initialized the process as a regular PostgreSQL background worker
/// process.
///
/// Inputs:
///   `tenant_id` and `timeline_id` can be NULL, if we're been launched in "non-Neon" mode,
///   where we use local storage instead of connecting to remote neon storage. That's
///   currently only used in some unit tests.
///
/// Result:
///   Returns pointer to CommunicatorWorkerProcessStruct, which is a handle to running
///   Rust tasks. The C code can use it to interact with the Rust parts. On failure, returns
///   None/NULL, and an error message is returned in *error_p
///
/// This is called only once in the process, so the returned struct, and error message in
/// case of failure, are simply leaked.
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_launch(
    tenant_id: *const c_char,
    timeline_id: *const c_char,
    error_p: *mut *const c_char,
) -> Option<&'static CommunicatorWorkerProcessStruct> {
    // Convert the arguments into more convenient Rust types
    let tenant_id = if tenant_id.is_null() {
        None
    } else {
        let cstr = unsafe { CStr::from_ptr(tenant_id) };
        Some(cstr.to_str().expect("assume UTF-8"))
    };
    let timeline_id = if timeline_id.is_null() {
        None
    } else {
        let cstr = unsafe { CStr::from_ptr(timeline_id) };
        Some(cstr.to_str().expect("assume UTF-8"))
    };

    // The `init` function does all the work.
    let result = main_loop::init(tenant_id, timeline_id);

    // On failure, return the error message to the C caller in *error_p.
    match result {
        Ok(worker_struct) => Some(worker_struct),
        Err(errmsg) => {
            let errmsg = CString::new(errmsg).expect("no nuls within error message");
            let errmsg = Box::leak(errmsg.into_boxed_c_str());
            let p: *const c_char = errmsg.as_ptr();

            unsafe { *error_p = p };
            None
        }
    }
}
