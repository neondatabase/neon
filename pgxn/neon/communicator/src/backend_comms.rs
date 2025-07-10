//! This module implements a request/response "slot" for submitting
//! requests from backends to the communicator process.
//!
//! NB: The "backend" side of this code runs in Postgres backend processes,
//! which means that it is not safe to use the 'tracing' crate for logging, nor
//! to launch threads or use tokio tasks!

use std::cell::UnsafeCell;
use std::sync::atomic::fence;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::neon_request::{NeonIORequest, NeonIOResult};

use atomic_enum::atomic_enum;

/// One request/response slot. Each backend has its own set of slots that it
/// uses.
///
/// This is the moral equivalent of PgAioHandle for Postgres AIO requests
/// Like PgAioHandle, try to keep this small.
///
/// There is an array of these in shared memory. Therefore, this must be Sized.
///
/// ## Lifecycle of a request
///
/// A slot is always owned by either the backend process or the communicator
/// process, depending on the 'state'. Only the owning process is allowed to
/// read or modify the slot, except for reading the 'state' itself to check who
/// owns it.
///
/// A slot begins in the Idle state, where it is owned by the backend process.
/// To submit a request, the backend process fills the slot with the request
/// data, and changes it to the Submitted state. After changing the state, the
/// slot is owned by the communicator process, and the backend is not allowed
/// to access it until the communicator process marks it as Completed.
///
/// When the communicator process sees that the slot is in Submitted state, it
/// starts to process the request. After processing the request, it stores the
/// result in the slot, and changes the state to Completed. It is now owned by
/// the backend process again, which may now read the result, and reuse the
/// slot for a new request.
///
/// For correctness of the above protocol, we really only need two states:
/// "owned by backend" and "owned by communicator process". But to help with
/// debugging and better assertions, there are a few more states. When the
/// backend starts to fill in the request details in the slot, it first sets the
/// state from Idle to Filling, and when it's done with that, from Filling to
/// Submitted. In the Filling state, the slot is still owned by the
/// backend. Similarly, when the communicator process starts to process a
/// request, it sets it to Processing state first, but the slot is still owned
/// by the communicator process.
///
/// This struct doesn't handle waking up the communicator process when a request
/// has been submitted or when a response is ready. The 'owner_procno' is used
/// for waking up the backend on completion, but that happens elsewhere.
pub struct NeonIORequestSlot {
    /// similar to PgAioHandleState
    state: AtomicNeonIORequestSlotState,

    /// The owning process's ProcNumber. The worker process uses this to set the
    /// process's latch on completion.
    ///
    /// (This could be calculated from num_neon_request_slots_per_backend and
    /// the index of this slot in the overall 'neon_requst_slots array'. But we
    /// prefer the communicator process to not know how the request slots are
    /// divided between the backends.)
    owner_procno: AtomicI32,

    /// SAFETY: This is modified by submit_request(), after it has established
    /// ownership of the slot by setting state from Idle to Filling
    request: UnsafeCell<NeonIORequest>,

    /// Valid when state is Completed
    ///
    /// SAFETY: This is modified by RequestProcessingGuard::complete(). There
    /// can be only one RequestProcessingGuard outstanding for a slot at a time,
    /// because it is returned by start_processing_request() which checks the
    /// state, so RequestProcessingGuard has exclusive access to the slot.
    result: UnsafeCell<NeonIOResult>,
}

// The protocol described in the "Lifecycle of a request" section above ensures
// the safe access to the fields
unsafe impl Send for NeonIORequestSlot {}
unsafe impl Sync for NeonIORequestSlot {}

impl Default for NeonIORequestSlot {
    fn default() -> NeonIORequestSlot {
        NeonIORequestSlot {
            owner_procno: AtomicI32::new(-1),
            request: UnsafeCell::new(NeonIORequest::Empty),
            result: UnsafeCell::new(NeonIOResult::Empty),
            state: AtomicNeonIORequestSlotState::new(NeonIORequestSlotState::Idle),
        }
    }
}

#[atomic_enum]
#[derive(Eq, PartialEq)]
pub enum NeonIORequestSlotState {
    Idle,

    /// Backend is filling in the request
    Filling,

    /// Backend has submitted the request to the communicator, but the
    /// communicator process has not yet started processing it.
    Submitted,

    /// Communicator is processing the request
    Processing,

    /// Communicator has completed the request, and the 'result' field is now
    /// valid, but the backend has not read the result yet.
    Completed,
}

impl NeonIORequestSlot {
    /// Write a request to the slot, and mark it as Submitted.
    ///
    /// Note: This does not wake up the worker process to actually process
    /// the request. It's the caller's responsibility to do that.
    pub fn submit_request(&self, request: &NeonIORequest, proc_number: i32) {
        // Verify that the slot is in Idle state previously, and put it in
        // Filling state.
        //
        // XXX: This step isn't strictly necessary. Assuming the caller didn't
        // screw up and try to use a slot that's already in use, we could fill
        // the slot and switch it directly from Idle to Submitted state.
        if let Err(s) = self.state.compare_exchange(
            NeonIORequestSlotState::Idle,
            NeonIORequestSlotState::Filling,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            panic!("unexpected state in request slot: {s:?}");
        }

        // This fence synchronizes-with store/swap in `communicator_process_main_loop`.
        fence(Ordering::Acquire);

        self.owner_procno.store(proc_number, Ordering::Relaxed);
        unsafe { *self.request.get() = *request }
        self.state
            .store(NeonIORequestSlotState::Submitted, Ordering::Release);
    }

    pub fn get_state(&self) -> NeonIORequestSlotState {
        self.state.load(Ordering::Relaxed)
    }

    pub fn try_get_result(&self) -> Option<NeonIOResult> {
        // FIXME: ordering?
        let state = self.state.load(Ordering::Relaxed);
        if state == NeonIORequestSlotState::Completed {
            // This fence synchronizes-with store/swap in `communicator_process_main_loop`.
            fence(Ordering::Acquire);
            let result = unsafe { *self.result.get() };
            self.state.store(NeonIORequestSlotState::Idle, Ordering::Relaxed);
            Some(result)
        } else {
            None
        }
    }

    /// Read the IO request from the slot indicated in the wakeup
    pub fn start_processing_request<'a>(&'a self) -> Option<RequestProcessingGuard<'a>> {
        // XXX: using compare_exchange for this is not strictly necessary, as long as
        // the communicator process has _some_ means of tracking which requests it's
        // already processing. That could be a flag somewhere in communicator's private
        // memory, for example.
        if let Err(s) = self.state.compare_exchange(
            NeonIORequestSlotState::Submitted,
            NeonIORequestSlotState::Processing,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            // FIXME surprising state. This is unexpected at the moment, but if we
            // started to process requests more aggressively, without waiting for the
            // read from the pipe, then this could happen
            panic!("unexpected state in request slot: {s:?}");
        }
        fence(Ordering::Acquire);

        Some(RequestProcessingGuard(self))
    }
}

/// [`NeonIORequestSlot::start_processing_request`] returns this guard object to
/// indicate that the the caller now "owns" the slot, until it calls
/// [`RequestProcessingGuard::completed`].
///
/// TODO: implement Drop on this, to mark the request as Aborted or Errored
/// if [`RequestProcessingGuard::completed`] is not called.
pub struct RequestProcessingGuard<'a>(&'a NeonIORequestSlot);

unsafe impl<'a> Send for RequestProcessingGuard<'a> {}
unsafe impl<'a> Sync for RequestProcessingGuard<'a> {}

impl<'a> RequestProcessingGuard<'a> {
    pub fn get_request(&self) -> &NeonIORequest {
        unsafe { &*self.0.request.get() }
    }

    pub fn get_owner_procno(&self) -> i32 {
        self.0.owner_procno.load(Ordering::Relaxed)
    }

    pub fn completed(self, result: NeonIOResult) {
        // Store the result to the slot.
        unsafe {
            *self.0.result.get() = result;
        };

        // Mark the request as completed. After that, we no longer have
        // ownership of the slot, and must not modify it.
        let old_state = self
            .0
            .state
            .swap(NeonIORequestSlotState::Completed, Ordering::Release);
        assert!(old_state == NeonIORequestSlotState::Processing);
    }
}
