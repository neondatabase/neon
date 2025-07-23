//! This code runs in each backend process. That means that launching Rust threads, panicking
//! etc. is forbidden!

use std::os::fd::OwnedFd;

use crate::backend_comms::NeonIORequestSlot;
use crate::init::CommunicatorInitStruct;
use crate::integrated_cache::{BackendCacheReadOp, IntegratedCacheReadAccess};
use crate::neon_request::{CCachedGetPageVResult, COid};
use crate::neon_request::{NeonIORequest, NeonIOResult};

pub struct CommunicatorBackendStruct<'t> {
    my_proc_number: i32,

    neon_request_slots: &'t [NeonIORequestSlot],

    submission_pipe_write_fd: OwnedFd,

    pending_cache_read_op: Option<BackendCacheReadOp<'t>>,

    integrated_cache: &'t IntegratedCacheReadAccess<'t>,
}

#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_backend_init(
    cis: Box<CommunicatorInitStruct>,
    my_proc_number: i32,
) -> &'static mut CommunicatorBackendStruct<'static> {
    if my_proc_number < 0 {
        panic!("cannot attach to communicator shared memory with procnumber {my_proc_number}");
    }

    let integrated_cache = Box::leak(Box::new(cis.integrated_cache_init_struct.backend_init()));

    let bs: &'static mut CommunicatorBackendStruct =
        Box::leak(Box::new(CommunicatorBackendStruct {
            my_proc_number,
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
/// The requested slot must be free, or this panics.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_start_io_request(
    bs: &'_ mut CommunicatorBackendStruct,
    slot_idx: i32,
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
    bs.start_neon_io_request(slot_idx, request);

    slot_idx
}

#[unsafe(no_mangle)]
pub extern "C" fn bcomm_start_get_page_v_request(
    bs: &mut CommunicatorBackendStruct,
    slot_idx: i32,
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
    bs.start_neon_io_request(slot_idx, request);

    slot_idx
}

/// Check if a request has completed. Returns:
///
/// -1 if the request is still being processed
/// 0 on success
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_poll_request_completion(
    bs: &mut CommunicatorBackendStruct,
    request_slot_idx: u32,
    result_p: &mut NeonIOResult,
) -> i32 {
    match bs.neon_request_slots[request_slot_idx as usize].try_get_result() {
        None => -1, // still processing
        Some(result) => {
            *result_p = result;
            0
        }
    }
}

/// Check if a request has completed. Returns:
///
/// 'false' if the slot is Idle. The backend process has ownership.
/// 'true' if the slot is busy, and should be polled for result.
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_get_request_slot_status(
    bs: &mut CommunicatorBackendStruct,
    request_slot_idx: u32,
) -> bool {
    use crate::backend_comms::NeonIORequestSlotState;
    match bs.neon_request_slots[request_slot_idx as usize].get_state() {
        NeonIORequestSlotState::Idle => false,
        NeonIORequestSlotState::Filling => {
            // 'false' would be the right result here. However, this
            // is a very transient state. The C code should never
            // leave a slot in this state, so if it sees that,
            // something's gone wrong and it's not clear what to do
            // with it.
            panic!("unexpected Filling state in request slot {request_slot_idx}");
        }
        NeonIORequestSlotState::Submitted => true,
        NeonIORequestSlotState::Processing => true,
        NeonIORequestSlotState::Completed => true,
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

/// Check if the local file cache contians the given block
#[unsafe(no_mangle)]
pub extern "C" fn bcomm_cache_contains(
    bs: &mut CommunicatorBackendStruct,
    spc_oid: COid,
    db_oid: COid,
    rel_number: u32,
    fork_number: u8,
    block_number: u32,
) -> bool {
    bs.integrated_cache.cache_contains_page(
        &pageserver_page_api::RelTag {
            spcnode: spc_oid,
            dbnode: db_oid,
            relnode: rel_number,
            forknum: fork_number,
        },
        block_number,
    )
}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct FileCacheIterator {
    next_bucket: u64,

    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
}

/// Iterate over LFC contents
#[allow(clippy::missing_safety_doc)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcomm_cache_iterate_begin(
    _bs: &mut CommunicatorBackendStruct,
    iter: *mut FileCacheIterator,
) {
    unsafe { (*iter).next_bucket = 0 };
}

#[allow(clippy::missing_safety_doc)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcomm_cache_iterate_next(
    bs: &mut CommunicatorBackendStruct,
    iter: *mut FileCacheIterator,
) -> bool {
    use crate::integrated_cache::GetBucketResult;
    loop {
        let next_bucket = unsafe { (*iter).next_bucket } as usize;
        match bs.integrated_cache.get_bucket(next_bucket) {
            GetBucketResult::Occupied(rel, blk) => {
                unsafe {
                    (*iter).spc_oid = rel.spcnode;
                    (*iter).db_oid = rel.dbnode;
                    (*iter).rel_number = rel.relnode;
                    (*iter).fork_number = rel.forknum;
                    (*iter).block_number = blk;

                    (*iter).next_bucket += 1;
                }
                break true;
            }
            GetBucketResult::Vacant => {
                unsafe {
                    (*iter).next_bucket += 1;
                }
                continue;
            }
            GetBucketResult::OutOfBounds => {
                break false;
            }
        }
    }
}

impl<'t> CommunicatorBackendStruct<'t> {
    /// The slot must be free, or this panics.
    pub(crate) fn start_neon_io_request(&mut self, request_slot_idx: i32, request: &NeonIORequest) {
        let my_proc_number = self.my_proc_number;

        self.neon_request_slots[request_slot_idx as usize].submit_request(request, my_proc_number);

        // Tell the communicator about it
        self.notify_about_request(request_slot_idx);
    }

    /// Send a wakeup to the communicator process
    fn notify_about_request(self: &CommunicatorBackendStruct<'t>, request_slot_idx: i32) {
        // wake up communicator by writing the idx to the submission pipe
        //

        // This can block, if the pipe is full. That should be very rare,
        // because the communicator tries hard to drain the pipe to prevent
        // that. Also, there's a natural upper bound on how many wakeups can be
        // queued up: there is only a limited number of request slots for each
        // backend.
        //
        // If it does block very briefly, that's not too serious.
        let idxbuf = request_slot_idx.to_ne_bytes();

        let _res = nix::unistd::write(&self.submission_pipe_write_fd, &idxbuf);
        // FIXME: check result, return any errors
    }
}
