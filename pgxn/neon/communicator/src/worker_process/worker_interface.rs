//! Functions called from the C code in the worker process

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char};
use std::path::PathBuf;

use crate::init::CommunicatorInitStruct;
use crate::worker_process::main_loop;
use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

use pageserver_client_grpc::ShardStripeSize;

/// Launch the communicator's tokio tasks, which do most of the work.
///
/// The caller has initialized the process as a regular PostgreSQL background worker
/// process. The shared memory segment used to communicate with the backends has been
/// allocated and initialized earlier, at postmaster startup, in
/// rcommunicator_shmem_init().
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
pub extern "C" fn communicator_worker_process_launch(
    cis: Box<CommunicatorInitStruct>,
    tenant_id: *const c_char,
    timeline_id: *const c_char,
    auth_token: *const c_char,
    shard_map: *mut *mut c_char,
    nshards: u32,
    stripe_size: u32,
    file_cache_path: *const c_char,
    initial_file_cache_size: u64,
    error_p: *mut *const c_char,
) -> Option<&'static CommunicatorWorkerProcessStruct<'static>> {
    tracing::warn!("starting threads in rust code");
    // Convert the arguments into more convenient Rust types
    let tenant_id = {
        let cstr = unsafe { CStr::from_ptr(tenant_id) };
        cstr.to_str().expect("assume UTF-8")
    };
    let timeline_id = {
        let cstr = unsafe { CStr::from_ptr(timeline_id) };
        cstr.to_str().expect("assume UTF-8")
    };
    let auth_token = if auth_token.is_null() {
        None
    } else {
        let cstr = unsafe { CStr::from_ptr(auth_token) };
        Some(cstr.to_str().expect("assume UTF-8"))
    };
    let file_cache_path = {
        if file_cache_path.is_null() {
            None
        } else {
            let c_str = unsafe { CStr::from_ptr(file_cache_path) };
            Some(PathBuf::from(c_str.to_str().unwrap()))
        }
    };
    let shard_map = shard_map_to_hash(nshards, shard_map);
    // FIXME: distinguish between unsharded, and sharded with 1 shard
    // Also, we might go from unsharded to sharded while the system
    // is running.
    let stripe_size = if stripe_size > 0 && nshards > 1 {
        Some(ShardStripeSize(stripe_size))
    } else {
        None
    };

    // The `init` function does all the work.
    let result = main_loop::init(
        *cis,
        tenant_id,
        timeline_id,
        auth_token,
        shard_map,
        stripe_size,
        initial_file_cache_size,
        file_cache_path,
    );

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

#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_process_launch_legacy(error_p: *mut *const c_char) -> bool {
    // The `init` function does all the work.
    let result = main_loop::init_legacy();

    // On failure, return the error message to the C caller in *error_p.
    match result {
        Ok(()) => true,
        Err(errmsg) => {
            let errmsg = CString::new(errmsg).expect("no nuls within error message");
            let errmsg = Box::leak(errmsg.into_boxed_c_str());
            let p: *const c_char = errmsg.as_ptr();

            unsafe { *error_p = p };
            false
        }
    }
}

/// Convert the "shard map" from an array of C strings, indexed by shard no to a rust HashMap
fn shard_map_to_hash(
    nshards: u32,
    shard_map: *mut *mut c_char,
) -> HashMap<utils::shard::ShardIndex, String> {
    use utils::shard::*;

    assert!(nshards <= u8::MAX as u32);

    let mut result: HashMap<ShardIndex, String> = HashMap::new();
    let mut p = shard_map;

    for i in 0..nshards {
        let c_str = unsafe { CStr::from_ptr(*p) };

        p = unsafe { p.add(1) };

        let s = c_str.to_str().unwrap();
        let k = if nshards > 1 {
            ShardIndex::new(ShardNumber(i as u8), ShardCount(nshards as u8))
        } else {
            ShardIndex::unsharded()
        };
        result.insert(k, s.into());
    }
    result
}

/// Inform the rust code about a configuration change
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_config_reload(
    proc_handle: &'static CommunicatorWorkerProcessStruct<'static>,
    file_cache_size: u64,
    shard_map: *mut *mut c_char,
    nshards: u32,
    stripe_size: u32,
) {
    proc_handle.cache.resize_file_cache(file_cache_size as u32);

    let shard_map = shard_map_to_hash(nshards, shard_map);
    let stripe_size = (nshards > 1).then_some(ShardStripeSize(stripe_size));
    proc_handle.update_shard_map(shard_map, stripe_size);
}
