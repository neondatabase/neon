use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use crate::CommunicatorInitStruct;
use crate::backend_interface::CommunicatorBackendStruct;

use atomic_enum::atomic_enum;

type Lsn = u64;
type Oid = u32;

/// This is the moral equivalent of PgAioHandle for Postgres AIO requests
///
/// Like PgAioHandle, try to keep this small
pub struct NeonIOHandle {
    // similar to PgAioHandleState
    _aborted: bool,

    pub owner_procno: i32,

    pub request: NeonIORequest,

    // raw result of the IO operation: The meaning of this depends on the request type
    pub result: u64,
    pub state: AtomicNeonIOHandleState,
}

impl Default for NeonIOHandle {
    fn default() -> NeonIOHandle {
        NeonIOHandle {
            _aborted: false,
            owner_procno: -1,
            request: NeonIORequest::Empty,
            result: 0,
            state: AtomicNeonIOHandleState::new(NeonIOHandleState::Idle),
        }
    }
}

#[atomic_enum]
#[derive(Eq, PartialEq)]
pub enum NeonIOHandleState {
    Idle,
    Submitted,
    Processing,
    Aborted,
    Completed,
}

#[derive(Copy, Clone, Debug)]
pub enum NeonIORequest {
    Empty,
    RelExists(RelExistsRequest),
    RelSize(RelSizeRequest),
    GetPage(GetPageRequest),
    DbSize(DbSizeRequest),
}

#[derive(Copy, Clone, Debug)]
pub struct RelExistsRequest {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

#[derive(Copy, Clone, Debug)]
pub struct RelSizeRequest {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

#[derive(Copy, Clone, Debug)]
pub struct GetPageRequest {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
    pub dest_ptr: usize,
    pub dest_size: u32,
}

#[derive(Copy, Clone, Debug)]
pub struct DbSizeRequest {
    pub db_oid: Oid,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

impl CommunicatorInitStruct {
    // safety:: fixme: it's possible to get two mutable referneces to same slot
    pub fn get_request_slot(&self, request_idx: u32) -> &mut NeonIOHandle {
        assert!(request_idx < self.num_neon_request_slots);

        let slot: &mut NeonIOHandle = unsafe {
            let slot = self.neon_request_slots.offset(request_idx as isize);
            &mut *slot
        };

        slot
    }
}

impl CommunicatorBackendStruct {
    // safety:: fixme: it's possible to get two mutable referneces to same slot
    fn get_request_slot(&mut self, request_idx: u32) -> &mut NeonIOHandle {
        let start_idx = self.my_proc_number as u32 * self.cis.num_neon_request_slots_per_backend;

        assert!(request_idx >= start_idx);
        assert!(request_idx < start_idx + self.cis.num_neon_request_slots_per_backend);

        self.cis.get_request_slot(request_idx)
    }

    /// Note: there's no guarantee on when the communicator might pick it up. You should ring
    /// the doorbell. But it might pick it up immediately.
    pub fn start_neon_request(&mut self, request: &NeonIORequest) -> i32 {
        let my_proc_number = self.my_proc_number;

        // Grab next free slot
        // FIXME: any guarantee that there will be any?
        let idx = self.next_neon_request_idx;

        let start_idx = self.my_proc_number as u32 * self.cis.num_neon_request_slots_per_backend;
        let end_idx =
            (self.my_proc_number + 1) as u32 * self.cis.num_neon_request_slots_per_backend;

        let next_idx = idx + 1;
        self.next_neon_request_idx = if next_idx == end_idx {
            start_idx
        } else {
            next_idx
        };

        let slot = self.get_request_slot(idx);

        let state = slot.state.load(Ordering::Relaxed);
        if state != NeonIOHandleState::Idle {
            // FIXME: it's pretty unexpected if it's running
            panic!();
        }

        // This fence synchronizes-with store/swap in `communicator_process_main_loop`.
        fence(Ordering::Acquire);

        slot.owner_procno = my_proc_number;
        slot.request = *request;
        slot.state
            .store(NeonIOHandleState::Submitted, Ordering::Release);

        return idx as i32;
    }

    pub fn poll_request_completion(&mut self, request_idx: u32) -> (NeonIOHandleState, u64) {
        let slot = self.get_request_slot(request_idx);

        let state = slot.state.load(Ordering::Relaxed);
        let result = if state == NeonIOHandleState::Completed {
            // This fence synchronizes-with store/swap in `communicator_process_main_loop`.
            fence(Ordering::Acquire);
            let result = slot.result;
            slot.state.store(NeonIOHandleState::Idle, Ordering::Relaxed);
            result
        } else {
            0
        };

        (state, result)
    }
}
