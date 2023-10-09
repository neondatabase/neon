#![allow(dead_code)]

use crate::bindings::uint32;
use crate::bindings::walproposer_api;
use crate::bindings::PGAsyncReadResult;
use crate::bindings::PGAsyncWriteResult;
use crate::bindings::Safekeeper;
use crate::bindings::Size;
use crate::bindings::TimeLineID;
use crate::bindings::TimestampTz;
use crate::bindings::WalProposer;
use crate::bindings::WalProposerConnStatusType;
use crate::bindings::WalProposerConnectPollStatusType;
use crate::bindings::WalProposerExecStatusType;
use crate::bindings::WalproposerShmemState;
use crate::bindings::XLogRecPtr;
use crate::walproposer::ApiImpl;

extern "C" fn get_shmem_state(wp: *mut WalProposer) -> *mut WalproposerShmemState {
    std::ptr::null_mut()
}

extern "C" fn start_streaming(wp: *mut WalProposer, startpos: XLogRecPtr) {}

extern "C" fn get_flush_rec_ptr(wp: *mut WalProposer) -> XLogRecPtr {
    0
}

extern "C" fn get_current_timestamp(wp: *mut WalProposer) -> TimestampTz {
    0
}

extern "C" fn conn_error_message(sk: *mut Safekeeper) -> *mut ::std::os::raw::c_char {
    std::ptr::null_mut()
}

extern "C" fn conn_status(sk: *mut Safekeeper) -> WalProposerConnStatusType {
    0
}

extern "C" fn conn_connect_start(sk: *mut Safekeeper) {}

extern "C" fn conn_connect_poll(sk: *mut Safekeeper) -> WalProposerConnectPollStatusType {
    0
}

extern "C" fn conn_send_query(sk: *mut Safekeeper, query: *mut ::std::os::raw::c_char) -> bool {
    false
}

extern "C" fn conn_get_query_result(sk: *mut Safekeeper) -> WalProposerExecStatusType {
    0
}

extern "C" fn conn_flush(sk: *mut Safekeeper) -> ::std::os::raw::c_int {
    0
}

extern "C" fn conn_finish(sk: *mut Safekeeper) {}

extern "C" fn conn_async_read(
    sk: *mut Safekeeper,
    buf: *mut *mut ::std::os::raw::c_char,
    amount: *mut ::std::os::raw::c_int,
) -> PGAsyncReadResult {
    0
}

extern "C" fn conn_async_write(
    sk: *mut Safekeeper,
    buf: *const ::std::os::raw::c_void,
    size: usize,
) -> PGAsyncWriteResult {
    0
}

extern "C" fn conn_blocking_write(
    sk: *mut Safekeeper,
    buf: *const ::std::os::raw::c_void,
    size: usize,
) -> bool {
    false
}

extern "C" fn recovery_download(
    sk: *mut Safekeeper,
    timeline: TimeLineID,
    startpos: XLogRecPtr,
    endpos: XLogRecPtr,
) -> bool {
    false
}

extern "C" fn wal_read(
    sk: *mut Safekeeper,
    buf: *mut ::std::os::raw::c_char,
    startptr: XLogRecPtr,
    count: Size,
) {
}

extern "C" fn wal_reader_allocate(sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).wal_reader_allocate(&mut (*sk));
    }
}

extern "C" fn free_event_set(wp: *mut WalProposer) {

}

extern "C" fn init_event_set(wp: *mut WalProposer) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).init_event_set(&mut (*wp));
    }
}

extern "C" fn update_event_set(sk: *mut Safekeeper, events: uint32) {

}

extern "C" fn add_safekeeper_event_set(sk: *mut Safekeeper, events: uint32) {

}

extern "C" fn wait_event_set(
    wp: *mut WalProposer,
    timeout: ::std::os::raw::c_long,
    sk: *mut *mut Safekeeper,
    events: *mut uint32,
) -> ::std::os::raw::c_int {
    0
}

extern "C" fn strong_random(
    wp: *mut WalProposer,
    buf: *mut ::std::os::raw::c_void,
    len: usize,
) -> bool {
    let buf = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, len) };

    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).strong_random(buf)
    }
}

extern "C" fn get_redo_start_lsn(wp: *mut WalProposer) -> XLogRecPtr {
    0
}

extern "C" fn finish_sync_safekeepers(wp: *mut WalProposer, lsn: XLogRecPtr) {

}

extern "C" fn process_safekeeper_feedback(wp: *mut WalProposer, commit_lsn: XLogRecPtr) {

}

extern "C" fn confirm_wal_streamed(wp: *mut WalProposer, lsn: XLogRecPtr) {

}

pub(crate) fn create_api() -> walproposer_api {
    walproposer_api {
        get_shmem_state: Some(get_shmem_state),
        start_streaming: Some(start_streaming),
        get_flush_rec_ptr: Some(get_flush_rec_ptr),
        get_current_timestamp: Some(get_current_timestamp),
        conn_error_message: Some(conn_error_message),
        conn_status: Some(conn_status),
        conn_connect_start: Some(conn_connect_start),
        conn_connect_poll: Some(conn_connect_poll),
        conn_send_query: Some(conn_send_query),
        conn_get_query_result: Some(conn_get_query_result),
        conn_flush: Some(conn_flush),
        conn_finish: Some(conn_finish),
        conn_async_read: Some(conn_async_read),
        conn_async_write: Some(conn_async_write),
        conn_blocking_write: Some(conn_blocking_write),
        recovery_download: Some(recovery_download),
        wal_read: Some(wal_read),
        wal_reader_allocate: Some(wal_reader_allocate),
        free_event_set: Some(free_event_set),
        init_event_set: Some(init_event_set),
        update_event_set: Some(update_event_set),
        add_safekeeper_event_set: Some(add_safekeeper_event_set),
        wait_event_set: Some(wait_event_set),
        strong_random: Some(strong_random),
        get_redo_start_lsn: Some(get_redo_start_lsn),
        finish_sync_safekeepers: Some(finish_sync_safekeepers),
        process_safekeeper_feedback: Some(process_safekeeper_feedback),
        confirm_wal_streamed: Some(confirm_wal_streamed),
    }
}
