//! C callbacks to PostgreSQL facilities that the neon extension needs to provide. These
//! are implemented in `neon/pgxn/communicator_process.c`. The function signatures better
//! match!
//!
//! These are called from the communicator threads! Careful what you do, most Postgres
//! functions are not safe to call in that context.

#[cfg(not(test))]
unsafe extern "C" {
    pub fn callback_set_my_latch_unsafe();
    pub fn callback_get_lfc_metrics_unsafe() -> LfcMetrics;
}

// Compile unit tests with dummy versions of the functions. Unit tests cannot call back
// into the C code. (As of this writing, no unit tests even exists in the communicator
// package, but the code coverage build still builds these and tries to link with the
// external C code.)
#[cfg(test)]
unsafe fn callback_set_my_latch_unsafe() {
    panic!("not usable in unit tests");
}
#[cfg(test)]
unsafe fn callback_get_lfc_metrics_unsafe() -> LfcMetrics {
    panic!("not usable in unit tests");
}

// safe wrappers

pub(super) fn callback_set_my_latch() {
    unsafe { callback_set_my_latch_unsafe() };
}

pub(super) fn callback_get_lfc_metrics() -> LfcMetrics {
    unsafe { callback_get_lfc_metrics_unsafe() }
}

/// Return type of the callback_get_lfc_metrics() function.
#[repr(C)]
pub struct LfcMetrics {
    pub lfc_cache_size_limit: i64,
    pub lfc_hits: i64,
    pub lfc_misses: i64,
    pub lfc_used: i64,
    pub lfc_writes: i64,

    // working set size looking back 1..60 minutes.
    //
    // Index 0 is the size of the working set accessed within last 1 minute,
    // index 59 is the size of the working set accessed within last 60 minutes.
    pub lfc_approximate_working_set_size_windows: [i64; 60],
}
