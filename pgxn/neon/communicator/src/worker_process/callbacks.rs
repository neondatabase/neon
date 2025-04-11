//! C callbacks to PostgreSQL facilities that the neon extension needs
//! to provide. These are implemented in `neon/pgxn/communicator_new.c`.
//! The function signatures better match!
//!
//! These are called from the communicator threads! Careful what you do, most
//! Postgres functions are not safe to call in that context.

unsafe extern "C" {
    pub fn notify_proc_unsafe(procno: std::ffi::c_int);
    pub fn callback_set_my_latch_unsafe();
}

// safe wrappers

pub(super) fn notify_proc(procno: std::ffi::c_int) {
    unsafe { notify_proc_unsafe(procno) };
}

pub(super) fn callback_set_my_latch() {
    unsafe { callback_set_my_latch_unsafe() };
}
