//! This code runs in each backend process. That means that launching Rust threads, panicking
//! etc. is forbidden!

use crate::CommunicatorInitStruct;
use crate::backend_comms::NeonIOHandleState;
use crate::neon_request::{NeonIORequest, NeonIOResult};

pub struct CommunicatorBackendStruct {
    pub my_proc_number: i32,

    pub cis: CommunicatorInitStruct,

    pub next_neon_request_idx: u32,
}

#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_backend_init(
    cis: *const CommunicatorInitStruct,
    my_proc_number: i32,
) -> *mut CommunicatorBackendStruct {
    let cis = unsafe { &*cis };

    let start_idx = my_proc_number as u32 * cis.num_neon_request_slots_per_backend;

    let bs: &'static mut CommunicatorBackendStruct =
        Box::leak(Box::new(CommunicatorBackendStruct {
            my_proc_number,
            cis: cis.clone(),
            next_neon_request_idx: start_idx,
        }));
    bs
}

/// Start a request. You can poll for its completion and get the result by
/// calling bcomm_poll_dbsize_request_completion(). The communicator will wake
/// us up by setting our process latch, so to wait for the completion, wait on
/// the latch and call bcomm_poll_dbsize_request_completion() every time the
/// latch is set.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_start_io_request(
    bs: *mut CommunicatorBackendStruct,
    request: *const NeonIORequest,
) -> i32 {
    let bs = unsafe { &mut *bs };
    let request = unsafe { &*request };

    // Create neon request and submit it
    let request_idx = bs.start_neon_request(request);

    // Tell the communicator about it
    bs.submit_request(request_idx);

    return request_idx;
}

/// Check if a request has completed. Returns:
///
/// -1 if the request is still being processed
/// 0 if the relation does not exist
/// 1 if the relation exists
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_poll_request_completion(
    bs: *mut CommunicatorBackendStruct,
    request_idx: u32,
    result_p: *mut *const NeonIOResult,
) -> i32 {
    let bs = unsafe { &mut *bs };

    let (state, result) = bs.poll_request_completion(request_idx);

    if state == NeonIOHandleState::Completed {
        unsafe { *result_p = result}
        0
    } else {
        -1
    }
}

impl CommunicatorBackendStruct {
    /// Send a wakeup to the communicator process
    fn submit_request(self: &CommunicatorBackendStruct, request_idx: i32) {
        // wake up communicator by writing the idx to the submission pipe
        //
        // This can block, if the pipe is full. That should be very rare,
        // because the communicator tries hard to drain the pipe to prevent
        // that. Also, there's a natural upper bound on how many wakeups can be
        // queued up: there is only a limited number of request slots for each
        // backend.
        //
        // If it does block very briefly, that's not too serious.
        let idxbuf = request_idx.to_ne_bytes();
        let _res = nix::unistd::write(self.cis.submission_pipe_write_fd, &idxbuf);
        // FIXME: check result, return any errors
    }
}
