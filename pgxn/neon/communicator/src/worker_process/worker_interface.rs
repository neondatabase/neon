//! Functions called from the C code in the worker process

use std::collections::HashMap;
use std::ffi::{CStr, c_char};
use std::path::PathBuf;

use tracing::error;

use crate::init::CommunicatorInitStruct;
use crate::worker_process::main_loop;
use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

/// Launch the communicator's tokio tasks, which do most of the work.
///
/// The caller has initialized the process as a regular PostgreSQL
/// background worker process. The shared memory segment used to
/// communicate with the backends has been allocated and initialized
/// earlier, at postmaster startup, in rcommunicator_shmem_init().
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_process_launch(
    cis: Box<CommunicatorInitStruct>,
    tenant_id: *const c_char,
    timeline_id: *const c_char,
    auth_token: *const c_char,
    shard_map: *mut *mut c_char,
    nshards: u32,
    file_cache_path: *const c_char,
    initial_file_cache_size: u64,
) -> &'static CommunicatorWorkerProcessStruct<'static> {
    // Convert the arguments into more convenient Rust types
    let tenant_id = unsafe { CStr::from_ptr(tenant_id) }.to_str().unwrap();
    let timeline_id = unsafe { CStr::from_ptr(timeline_id) }.to_str().unwrap();
    let auth_token = unsafe { auth_token.as_ref() }.map(|s| s.to_string());
    let file_cache_path = {
        if file_cache_path.is_null() {
            None
        } else {
            let c_str = unsafe { CStr::from_ptr(file_cache_path) };
            Some(PathBuf::from(c_str.to_str().unwrap()))
        }
    };
    let shard_map = parse_shard_map(nshards, shard_map);

    // start main loop
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("communicator thread")
        .build()
        .unwrap();

    let worker_struct = runtime.block_on(main_loop::init(
        cis,
        tenant_id.to_string(),
        timeline_id.to_string(),
        auth_token,
        shard_map,
        initial_file_cache_size,
        file_cache_path,
    ));
    let worker_struct = Box::leak(Box::new(worker_struct));

    let main_loop_handle = runtime.spawn(worker_struct.run());

    runtime.spawn(async {
        let err = main_loop_handle.await.unwrap_err();
        error!("error: {err:?}");
    });

    runtime.block_on(worker_struct.launch_exporter_task());

    // keep the runtime running after we exit this function
    Box::leak(Box::new(runtime));

    worker_struct
}

/// Convert the "shard map" from an array of C strings, indexed by shard no to a rust HashMap
fn parse_shard_map(
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
) {
    proc_handle.cache.resize_file_cache(file_cache_size as u32);
}
