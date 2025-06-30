//! This code runs in each backend process. That means that launching Rust threads, panicking
//! etc. is forbidden!

use std::os::fd::OwnedFd;

use crate::backend_comms::NeonIOHandle;
use crate::init::CommunicatorInitStruct;
use crate::integrated_cache::{BackendCacheReadOp, IntegratedCacheReadAccess};
use crate::neon_request::CCachedGetPageVResult;
use crate::neon_request::{NeonIORequest, NeonIOResult};

pub struct CommunicatorBackendStruct<'t> {
    my_proc_number: i32,

    next_neon_request_idx: u32,

    my_start_idx: u32, // First request slot that belongs to this backend
    my_end_idx: u32,   // end + 1 request slot that belongs to this backend

    neon_request_slots: &'t [NeonIOHandle],

    submission_pipe_write_fd: OwnedFd,

    pending_cache_read_op: Option<BackendCacheReadOp<'t>>,

    integrated_cache: &'t IntegratedCacheReadAccess<'t>,
}

#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_backend_init(
    cis: Box<CommunicatorInitStruct>,
    my_proc_number: i32,
) -> &'static mut CommunicatorBackendStruct<'static> {
    let start_idx = my_proc_number as u32 * cis.num_neon_request_slots_per_backend;
    let end_idx = start_idx + cis.num_neon_request_slots_per_backend;

    let integrated_cache = Box::leak(Box::new(cis.integrated_cache_init_struct.backend_init()));

    let bs: &'static mut CommunicatorBackendStruct =
        Box::leak(Box::new(CommunicatorBackendStruct {
            my_proc_number,
            next_neon_request_idx: start_idx,
            my_start_idx: start_idx,
            my_end_idx: end_idx,
            neon_request_slots: cis.neon_request_slots,

            submission_pipe_write_fd: cis.submission_pipe_write_fd,
            pending_cache_read_op: None,

            integrated_cache,
        }));
    bs
}

/// Start a request. You can poll for its completion and get the result by
/// calling bcomm_poll_dbsize_request_completion(). The communicator will wake
/// us up by setting our process latch, so to wait for the completion, wait on
/// the latch and call bcomm_poll_dbsize_request_completion() every time the
/// latch is set.
///
/// Safety: The C caller must ensure that the references are valid.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_start_io_request(
    bs: &'_ mut CommunicatorBackendStruct,
    request: &NeonIORequest,
    immediate_result_ptr: &mut NeonIOResult,
) -> i32 {
    assert!(bs.pending_cache_read_op.is_none());

    // Check if the request can be satisfied from the cache first
    if let NeonIORequest::RelSize(req) = request {
        if let Some(nblocks) = bs.integrated_cache.get_rel_size(&req.reltag()) {
            *immediate_result_ptr = NeonIOResult::RelSize(nblocks);
            return -1;
        }
    }

    // Create neon request and submit it
    let request_idx = bs.start_neon_request(request);

    // Tell the communicator about it
    bs.submit_request(request_idx);

    request_idx
}

#[unsafe(no_mangle)]
pub extern "C" fn bcomm_start_get_page_v_request(
    bs: &mut CommunicatorBackendStruct,
    request: &NeonIORequest,
    immediate_result_ptr: &mut CCachedGetPageVResult,
) -> i32 {
    let NeonIORequest::GetPageV(get_pagev_request) = request else {
        panic!("invalid request passed to bcomm_start_get_page_v_request()");
    };
    assert!(matches!(request, NeonIORequest::GetPageV(_)));
    assert!(bs.pending_cache_read_op.is_none());

    // Check if the request can be satisfied from the cache first
    let mut all_cached = true;
    let mut read_op = bs.integrated_cache.start_read_op();
    for i in 0..get_pagev_request.nblocks {
        if let Some(cache_block) = read_op.get_page(
            &get_pagev_request.reltag(),
            get_pagev_request.block_number + i as u32,
        ) {
            immediate_result_ptr.cache_block_numbers[i as usize] = cache_block;
        } else {
            // not found in cache
            all_cached = false;
            break;
        }
    }
    if all_cached {
        bs.pending_cache_read_op = Some(read_op);
        return -1;
    }

    // Create neon request and submit it
    let request_idx = bs.start_neon_request(request);

    // Tell the communicator about it
    bs.submit_request(request_idx);

    request_idx
}

/// Check if a request has completed. Returns:
///
/// -1 if the request is still being processed
/// 0 on success
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_poll_request_completion(
    bs: &mut CommunicatorBackendStruct,
    request_idx: u32,
    result_p: &mut NeonIOResult,
) -> i32 {
    match bs.neon_request_slots[request_idx as usize].try_get_result() {
        None => -1, // still processing
        Some(result) => {
            *result_p = result;
            0
        }
    }
}

// LFC functions

/// Finish a local file cache read
///
//
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_finish_cache_read(bs: &mut CommunicatorBackendStruct) -> bool {
    if let Some(op) = bs.pending_cache_read_op.take() {
        op.finish()
    } else {
        panic!("bcomm_finish_cache_read() called with no cached read pending");
    }
}

impl<'t> CommunicatorBackendStruct<'t> {
    /// Send a wakeup to the communicator process
    fn submit_request(self: &CommunicatorBackendStruct<'t>, request_idx: i32) {
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

        let _res = nix::unistd::write(&self.submission_pipe_write_fd, &idxbuf);
        // FIXME: check result, return any errors
    }

    /// Note: there's no guarantee on when the communicator might pick it up. You should ring
    /// the doorbell. But it might pick it up immediately.
    pub(crate) fn start_neon_request(&mut self, request: &NeonIORequest) -> i32 {
        let my_proc_number = self.my_proc_number;

        // Grab next free slot
        // FIXME: any guarantee that there will be any?
        let idx = self.next_neon_request_idx;

        let next_idx = idx + 1;
        self.next_neon_request_idx = if next_idx == self.my_end_idx {
            self.my_start_idx
        } else {
            next_idx
        };

        self.neon_request_slots[idx as usize].fill_request(request, my_proc_number);

        idx as i32
    }
}
