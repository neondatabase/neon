use crate::bindings::WalProposer;
use crate::bindings::XLogReaderState;
use crate::bindings::walproposer_api;

/// Rust high-level wrapper for C-like API.
pub trait ApiImpl {
    fn strong_random(&self, buf: &mut [u8]) -> bool;
}

pub struct ExampleImpl {}

impl ApiImpl for ExampleImpl {
    fn strong_random(&self, buf: &mut [u8]) -> bool {
        println!("hello from strong random!");
        true
    }
}

extern "C" fn strong_random(wp: *mut WalProposer, buf: *mut ::std::os::raw::c_void, len: usize) -> bool {
    let buf = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, len) };

    unsafe { 
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).strong_random(buf)
    }
}

extern "C" fn wal_reader_allocate(wp: *mut WalProposer) -> *mut XLogReaderState {
    std::ptr::null_mut()
}

extern "C" fn init_event_set(wp: *mut WalProposer) {
    
}

pub(crate) fn create_api() -> walproposer_api {
    walproposer_api {
        get_shmem_state: None,
        start_streaming: None,
        get_flush_rec_ptr: None,
        get_current_timestamp: None,
        conn_error_message: None,
        conn_status: None,
        conn_connect_start: None,
        conn_connect_poll: None,
        conn_send_query: None,
        conn_get_query_result: None,
        conn_flush: None,
        conn_finish: None,
        conn_async_read: None,
        conn_async_write: None,
        conn_blocking_write: None,
        recovery_download: None,
        wal_read: None,
        wal_reader_allocate: Some(wal_reader_allocate),
        free_event_set: None,
        init_event_set: Some(init_event_set),
        update_event_set: None,
        add_safekeeper_event_set: None,
        wait_event_set: None,
        strong_random: Some(strong_random),
        get_redo_start_lsn: None,
        finish_sync_safekeepers: None,
        process_safekeeper_feedback: None,
        confirm_wal_streamed: None,
    }
}
