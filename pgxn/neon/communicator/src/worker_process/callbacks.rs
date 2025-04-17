//! C callbacks to PostgreSQL facilities that the neon extension needs
//! to provide. These are implemented in `neon/pgxn/communicator_new.c`.
//! The function signatures better match!
//!
//! These are called from the communicator threads! Careful what you do, most
//! Postgres functions are not safe to call in that context.
use std::ffi::{CString, c_char};

unsafe extern "C" {
    pub fn notify_proc_unsafe(procno: std::ffi::c_int);
    pub fn elog_log_unsafe(s: *const c_char);
}

// safe wrappers

pub(super) fn notify_proc(procno: std::ffi::c_int) {
    unsafe { notify_proc_unsafe(procno) };
}

pub(super) fn elog_log(s: &str) {
    let s: CString = CString::new(s).unwrap();
    unsafe { elog_log_unsafe(s.as_c_str().as_ptr()) };
}
