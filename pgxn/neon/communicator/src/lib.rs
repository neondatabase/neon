//!
//! Three main parts:
//! - async tokio communicator core, which receives requests and processes them.
//! - Main loop and requests queues, which routes requests from backends to the core
//! - the per-backend glue code, which submits requests
//!

mod backend_interface;
mod init;
mod neon_request;
mod processor;
mod worker_process;

use neon_request::NeonIOHandle;

/// This struct is created in the postmaster process, and inherited to
/// the communicator process and all backend processes through fork()
#[derive(Clone)]
pub struct CommunicatorInitStruct {
    #[allow(dead_code)]
    max_backends: u32,
    #[allow(dead_code)]
    shmem_ptr: *mut char,
    #[allow(dead_code)]
    shmem_size: u64,

    submission_pipe_read_fd: std::ffi::c_int,
    pub submission_pipe_write_fd: std::ffi::c_int,

    // Shared memory data structures
    pub num_neon_request_slots_per_backend: u32,

    // Array. FIXME: can this be a rust array for safety?
    pub num_neon_request_slots: u32,
    pub neon_request_slots: *mut NeonIOHandle,
}
unsafe impl Send for CommunicatorInitStruct {}
unsafe impl Sync for CommunicatorInitStruct {}
