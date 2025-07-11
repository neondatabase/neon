//! Three main parts:
//! - async tokio communicator core, which receives requests and processes them.
//! - Main loop and requests queues, which routes requests from backends to the core
//! - the per-backend glue code, which submits requests

mod backend_comms;

// mark this 'pub', because these functions are called from C code. Otherwise, the compiler
// complains about a bunch of structs and enum variants being unused, because it thinkgs
// the functions that use them are never called. There are some C-callable functions in
// other modules too, but marking this as pub is currently enough to silence the warnings
//
// TODO: perhaps collect *all* the extern "C" functions to one module?
pub mod backend_interface;

mod file_cache;
mod init;
mod integrated_cache;
mod neon_request;
mod worker_process;

mod global_allocator;

// FIXME: get this from postgres headers somehow
/// Size of a disk block: this also limits the size of a tuple. You can
/// set it bigger if you need bigger tuples (although `TOAST` should
/// reduce the need to have large tuples, since fields can be spread
/// across multiple tuples). [`BLCKSZ`] must be a power of `2`. The
/// maximum possible value of [`BLCKSZ`] is currently `2^15` (`32768`).
/// This is determined by the `15-bit` widths of the `lp_off` and
/// `lp_len` fields in ItemIdData (see `include/storage/itemid.h`).
/// Changing [`BLCKSZ`] requires an `initdb`.
pub const BLCKSZ: usize = 8192;

/// Define SLRU segment size. A page is the same [`BLCKSZ``] and is used
/// everywhere else in Postgres. The segment size can be chosen somewhat
/// arbitrarily; we make it `32` pages by default, or `256Kb`, i.e. 1
/// **million** transactions for `CLOG` or `64K` transactions for
/// `SUBTRANS`.
///
/// # Note
///
/// Because TransactionIds are 32 bits and wrap around at `0xFFFFFFFF``,
/// page numbering also wraps around at `0xFFFFFFFF/xxxx_XACTS_PER_PAGE`
/// (where `xxxx` is `CLOG` or `SUBTRANS`, respectively), and segment
/// numbering at
/// `0xFFFFFFFF/xxxx_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT`. We need
/// take no explicit notice of that fact in `slru.c`, except when
/// comparing segment and page numbers in SimpleLruTruncate
/// (see PagePrecedes()).
pub const SLRU_PAGES_PER_SEGMENT: usize = 32;
