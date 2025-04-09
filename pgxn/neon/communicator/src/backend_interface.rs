//! This code runs in each backend process. That means that launching Rust threads, panicking
//! etc. is forbidden!

use crate::CommunicatorInitStruct;
use crate::neon_request::NeonIOHandleState;
use crate::neon_request::{DbSizeRequest, NeonIORequest};

type Lsn = u64;
type Oid = u32;

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
pub extern "C" fn bcomm_start_dbsize_request(
    bs: *mut CommunicatorBackendStruct,
    db_oid: Oid,
    request_lsn: Lsn,
    not_modified_since: Lsn,
) -> i32 {
    let bs = unsafe { &mut *bs };

    // Create neon request and submit it
    let request_idx = bs.start_neon_request(&NeonIORequest::DbSize(DbSizeRequest {
        db_oid,
        request_lsn,
        not_modified_since,
    }));

    // Tell the communicator about it
    bs.submit_request(request_idx);

    return request_idx;
}

/// Check if a request has completed. If yes, returns 0, and stores the result
/// in *result_p. If the request is still being processe, returns -1.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_poll_dbsize_request_completion(
    bs: *mut CommunicatorBackendStruct,
    request_idx: u32,
    result_p: *mut u64,
) -> i32 {
    let bs = unsafe { &mut *bs };
    let result_ref = unsafe { &mut *result_p };

    // wait for response
    let (state, result) = bs.poll_request_completion(request_idx);

    if state == NeonIOHandleState::Completed {
        *result_ref = result;
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
