//! Initialization functions. These are executed in the postmaster process,
//! at different stages of server startup.
//!
//!
//! Communicator initialization steps:
//!
//! 1. At postmaster startup, before shared memory is allocated,
//!    rcommunicator_shmem_size() is called to get the amount of
//!    shared memory that this module needs.
//!
//! 2. Later, after the shared memory has been allocated,
//!    rcommunicator_shmem_init() is called to initialize the shmem
//!    area.
//!
//! Per process initialization:
//!
//! When a backend process starts up, it calls rcommunicator_backend_init().
//! In the communicator worker process, other functions are called, see
//! `worker_process` module.

use std::ffi::c_int;
use std::mem;

use crate::CommunicatorInitStruct;
use crate::neon_request::NeonIOHandle;

const NUM_NEON_REQUEST_SLOTS_PER_BACKEND: u32 = 5;

#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_shmem_size(max_backends: u32) -> u64 {
    let mut size = 0;

    let num_neon_request_slots = max_backends * NUM_NEON_REQUEST_SLOTS_PER_BACKEND;
    size += mem::size_of::<NeonIOHandle>() * num_neon_request_slots as usize;

    size as u64
}

/// Initialize the shared memory segment. Returns a backend-private
/// struct, which will be inherited by backend processes through fork
#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_shmem_init(
    submission_pipe_read_fd: c_int,
    submission_pipe_write_fd: c_int,
    max_backends: u32,
    shmem_ptr: *mut u8,
    shmem_size: u64,
) -> *mut CommunicatorInitStruct {
    let mut ptr = shmem_ptr;
    let neon_request_slots = ptr.cast::<NeonIOHandle>();
    let num_neon_request_slots_per_backend = NUM_NEON_REQUEST_SLOTS_PER_BACKEND;
    let num_neon_request_slots = max_backends * num_neon_request_slots_per_backend;
    unsafe {
        for i in 0..num_neon_request_slots {
            let slot: *mut NeonIOHandle = neon_request_slots.offset(i as isize);
            *slot = NeonIOHandle::default();
            ptr = ptr.byte_add(mem::size_of::<NeonIOHandle>());
        }

        assert!(ptr.addr() - shmem_ptr.addr() == shmem_size as usize);
    }
    let cis: &'static mut CommunicatorInitStruct = Box::leak(Box::new(CommunicatorInitStruct {
        max_backends,
        shmem_ptr,
        shmem_size,
        submission_pipe_read_fd,
        submission_pipe_write_fd,

        num_neon_request_slots_per_backend: NUM_NEON_REQUEST_SLOTS_PER_BACKEND,
        num_neon_request_slots,
        neon_request_slots,
    }));

    cis
}
