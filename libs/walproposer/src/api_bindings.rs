//! A C-Rust shim: defines implementation of C walproposer API, assuming wp
//! callback_data stores Box to some Rust implementation.

#![allow(dead_code)]

use std::ffi::CStr;
use std::ffi::CString;

use crate::bindings::uint32;
use crate::bindings::walproposer_api;
use crate::bindings::NeonWALReadResult;
use crate::bindings::PGAsyncReadResult;
use crate::bindings::PGAsyncWriteResult;
use crate::bindings::Safekeeper;
use crate::bindings::Size;
use crate::bindings::StringInfoData;
use crate::bindings::TimestampTz;
use crate::bindings::WalProposer;
use crate::bindings::WalProposerConnStatusType;
use crate::bindings::WalProposerConnectPollStatusType;
use crate::bindings::WalProposerExecStatusType;
use crate::bindings::WalproposerShmemState;
use crate::bindings::XLogRecPtr;
use crate::walproposer::ApiImpl;
use crate::walproposer::StreamingCallback;
use crate::walproposer::WaitResult;

extern "C" fn get_shmem_state(wp: *mut WalProposer) -> *mut WalproposerShmemState {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).get_shmem_state()
    }
}

extern "C-unwind" fn start_streaming(wp: *mut WalProposer, startpos: XLogRecPtr) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        let callback = StreamingCallback::new(wp);
        (*api).start_streaming(startpos, &callback);
    }
}

extern "C" fn get_flush_rec_ptr(wp: *mut WalProposer) -> XLogRecPtr {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).get_flush_rec_ptr()
    }
}

extern "C" fn update_donor(wp: *mut WalProposer, donor: *mut Safekeeper, donor_lsn: XLogRecPtr) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).update_donor(&mut (*donor), donor_lsn)
    }
}

extern "C" fn get_current_timestamp(wp: *mut WalProposer) -> TimestampTz {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).get_current_timestamp()
    }
}

extern "C" fn conn_error_message(sk: *mut Safekeeper) -> *mut ::std::os::raw::c_char {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        let msg = (*api).conn_error_message(&mut (*sk));
        let msg = CString::new(msg).unwrap();
        // TODO: fix leaking error message
        msg.into_raw()
    }
}

extern "C" fn conn_status(sk: *mut Safekeeper) -> WalProposerConnStatusType {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_status(&mut (*sk))
    }
}

extern "C" fn conn_connect_start(sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_connect_start(&mut (*sk))
    }
}

extern "C" fn conn_connect_poll(sk: *mut Safekeeper) -> WalProposerConnectPollStatusType {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_connect_poll(&mut (*sk))
    }
}

extern "C" fn conn_send_query(sk: *mut Safekeeper, query: *mut ::std::os::raw::c_char) -> bool {
    let query = unsafe { CStr::from_ptr(query) };
    let query = query.to_str().unwrap();

    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_send_query(&mut (*sk), query)
    }
}

extern "C" fn conn_get_query_result(sk: *mut Safekeeper) -> WalProposerExecStatusType {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_get_query_result(&mut (*sk))
    }
}

extern "C" fn conn_flush(sk: *mut Safekeeper) -> ::std::os::raw::c_int {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_flush(&mut (*sk))
    }
}

extern "C" fn conn_finish(sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_finish(&mut (*sk))
    }
}

extern "C" fn conn_async_read(
    sk: *mut Safekeeper,
    buf: *mut *mut ::std::os::raw::c_char,
    amount: *mut ::std::os::raw::c_int,
) -> PGAsyncReadResult {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;

        // This function has guarantee that returned buf will be valid until
        // the next call. So we can store a Vec in each Safekeeper and reuse
        // it on the next call.
        let mut inbuf = take_vec_u8(&mut (*sk).inbuf).unwrap_or_default();
        inbuf.clear();

        let result = (*api).conn_async_read(&mut (*sk), &mut inbuf);

        // Put a Vec back to sk->inbuf and return data ptr.
        *amount = inbuf.len() as i32;
        *buf = store_vec_u8(&mut (*sk).inbuf, inbuf);

        result
    }
}

extern "C" fn conn_async_write(
    sk: *mut Safekeeper,
    buf: *const ::std::os::raw::c_void,
    size: usize,
) -> PGAsyncWriteResult {
    unsafe {
        let buf = std::slice::from_raw_parts(buf as *const u8, size);
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_async_write(&mut (*sk), buf)
    }
}

extern "C" fn conn_blocking_write(
    sk: *mut Safekeeper,
    buf: *const ::std::os::raw::c_void,
    size: usize,
) -> bool {
    unsafe {
        let buf = std::slice::from_raw_parts(buf as *const u8, size);
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).conn_blocking_write(&mut (*sk), buf)
    }
}

extern "C-unwind" fn recovery_download(wp: *mut WalProposer, sk: *mut Safekeeper) -> bool {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;

        // currently `recovery_download` is always called right after election
        (*api).after_election(&mut (*wp));

        (*api).recovery_download(&mut (*wp), &mut (*sk))
    }
}

extern "C" fn wal_reader_allocate(sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).wal_reader_allocate(&mut (*sk));
    }
}

#[allow(clippy::unnecessary_cast)]
extern "C" fn wal_read(
    sk: *mut Safekeeper,
    buf: *mut ::std::os::raw::c_char,
    startptr: XLogRecPtr,
    count: Size,
    _errmsg: *mut *mut ::std::os::raw::c_char,
) -> NeonWALReadResult {
    unsafe {
        let buf = std::slice::from_raw_parts_mut(buf as *mut u8, count);
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        // TODO: errmsg is not forwarded
        (*api).wal_read(&mut (*sk), buf, startptr)
    }
}

extern "C" fn wal_reader_events(sk: *mut Safekeeper) -> uint32 {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).wal_reader_events(&mut (*sk))
    }
}

extern "C" fn init_event_set(wp: *mut WalProposer) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).init_event_set(&mut (*wp));
    }
}

extern "C" fn update_event_set(sk: *mut Safekeeper, events: uint32) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).update_event_set(&mut (*sk), events);
    }
}

extern "C" fn active_state_update_event_set(sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).active_state_update_event_set(&mut (*sk));
    }
}

extern "C" fn add_safekeeper_event_set(sk: *mut Safekeeper, events: uint32) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).add_safekeeper_event_set(&mut (*sk), events);
    }
}

extern "C" fn rm_safekeeper_event_set(sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*(*sk).wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).rm_safekeeper_event_set(&mut (*sk));
    }
}

extern "C-unwind" fn wait_event_set(
    wp: *mut WalProposer,
    timeout: ::std::os::raw::c_long,
    event_sk: *mut *mut Safekeeper,
    events: *mut uint32,
) -> ::std::os::raw::c_int {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        let result = (*api).wait_event_set(&mut (*wp), timeout);
        match result {
            WaitResult::Latch => {
                *event_sk = std::ptr::null_mut();
                *events = crate::bindings::WL_LATCH_SET;
                1
            }
            WaitResult::Timeout => {
                *event_sk = std::ptr::null_mut();
                // WaitEventSetWait returns 0 for timeout.
                *events = 0;
                0
            }
            WaitResult::Network(sk, event_mask) => {
                *event_sk = sk;
                *events = event_mask;
                1
            }
        }
    }
}

extern "C" fn strong_random(
    wp: *mut WalProposer,
    buf: *mut ::std::os::raw::c_void,
    len: usize,
) -> bool {
    unsafe {
        let buf = std::slice::from_raw_parts_mut(buf as *mut u8, len);
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).strong_random(buf)
    }
}

extern "C" fn get_redo_start_lsn(wp: *mut WalProposer) -> XLogRecPtr {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).get_redo_start_lsn()
    }
}

extern "C-unwind" fn finish_sync_safekeepers(wp: *mut WalProposer, lsn: XLogRecPtr) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).finish_sync_safekeepers(lsn)
    }
}

extern "C" fn process_safekeeper_feedback(wp: *mut WalProposer, sk: *mut Safekeeper) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        (*api).process_safekeeper_feedback(&mut (*wp), &mut (*sk));
    }
}

extern "C-unwind" fn log_internal(
    wp: *mut WalProposer,
    level: ::std::os::raw::c_int,
    line: *const ::std::os::raw::c_char,
) {
    unsafe {
        let callback_data = (*(*wp).config).callback_data;
        let api = callback_data as *mut Box<dyn ApiImpl>;
        let line = CStr::from_ptr(line);
        let line = line.to_str().unwrap();
        (*api).log_internal(&mut (*wp), Level::from(level as u32), line)
    }
}

#[derive(Debug, PartialEq)]
pub enum Level {
    Debug5,
    Debug4,
    Debug3,
    Debug2,
    Debug1,
    Log,
    Info,
    Notice,
    Warning,
    Error,
    Fatal,
    Panic,
    WPEvent,
}

impl Level {
    pub fn from(elevel: u32) -> Level {
        use crate::bindings::*;

        match elevel {
            DEBUG5 => Level::Debug5,
            DEBUG4 => Level::Debug4,
            DEBUG3 => Level::Debug3,
            DEBUG2 => Level::Debug2,
            DEBUG1 => Level::Debug1,
            LOG => Level::Log,
            INFO => Level::Info,
            NOTICE => Level::Notice,
            WARNING => Level::Warning,
            ERROR => Level::Error,
            FATAL => Level::Fatal,
            PANIC => Level::Panic,
            WPEVENT => Level::WPEvent,
            _ => panic!("unknown log level {}", elevel),
        }
    }
}

pub(crate) fn create_api() -> walproposer_api {
    walproposer_api {
        get_shmem_state: Some(get_shmem_state),
        start_streaming: Some(start_streaming),
        get_flush_rec_ptr: Some(get_flush_rec_ptr),
        update_donor: Some(update_donor),
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
        wal_reader_allocate: Some(wal_reader_allocate),
        wal_read: Some(wal_read),
        wal_reader_events: Some(wal_reader_events),
        init_event_set: Some(init_event_set),
        update_event_set: Some(update_event_set),
        active_state_update_event_set: Some(active_state_update_event_set),
        add_safekeeper_event_set: Some(add_safekeeper_event_set),
        rm_safekeeper_event_set: Some(rm_safekeeper_event_set),
        wait_event_set: Some(wait_event_set),
        strong_random: Some(strong_random),
        get_redo_start_lsn: Some(get_redo_start_lsn),
        finish_sync_safekeepers: Some(finish_sync_safekeepers),
        process_safekeeper_feedback: Some(process_safekeeper_feedback),
        log_internal: Some(log_internal),
    }
}

pub fn empty_shmem() -> crate::bindings::WalproposerShmemState {
    let empty_feedback = crate::bindings::PageserverFeedback {
        present: false,
        currentClusterSize: 0,
        last_received_lsn: 0,
        disk_consistent_lsn: 0,
        remote_consistent_lsn: 0,
        replytime: 0,
        shard_number: 0,
    };

    crate::bindings::WalproposerShmemState {
        propEpochStartLsn: crate::bindings::pg_atomic_uint64 { value: 0 },
        donor_name: [0; 64],
        donor_conninfo: [0; 1024],
        donor_lsn: 0,
        mutex: 0,
        mineLastElectedTerm: crate::bindings::pg_atomic_uint64 { value: 0 },
        backpressureThrottlingTime: crate::bindings::pg_atomic_uint64 { value: 0 },
        currentClusterSize: crate::bindings::pg_atomic_uint64 { value: 0 },
        shard_ps_feedback: [empty_feedback; 128],
        num_shards: 0,
        min_ps_feedback: empty_feedback,
    }
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Take ownership of `Vec<u8>` from StringInfoData.
#[allow(clippy::unnecessary_cast)]
pub(crate) fn take_vec_u8(pg: &mut StringInfoData) -> Option<Vec<u8>> {
    if pg.data.is_null() {
        return None;
    }

    let ptr = pg.data as *mut u8;
    let length = pg.len as usize;
    let capacity = pg.maxlen as usize;

    pg.data = std::ptr::null_mut();
    pg.len = 0;
    pg.maxlen = 0;

    unsafe { Some(Vec::from_raw_parts(ptr, length, capacity)) }
}

/// Store `Vec<u8>` in StringInfoData.
fn store_vec_u8(pg: &mut StringInfoData, vec: Vec<u8>) -> *mut ::std::os::raw::c_char {
    let ptr = vec.as_ptr() as *mut ::std::os::raw::c_char;
    let length = vec.len();
    let capacity = vec.capacity();

    assert!(pg.data.is_null());

    pg.data = ptr;
    pg.len = length as i32;
    pg.maxlen = capacity as i32;

    std::mem::forget(vec);

    ptr
}
