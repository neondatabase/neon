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

/// Name of the Unix Domain Socket that serves the metrics, and other APIs in the
/// future. This is within the Postgres data directory.
const NEON_COMMUNICATOR_SOCKET_NAME: &str = "neon-communicator.socket";

// FIXME: get this from postgres headers somehow
pub const BLCKSZ: usize = 8192;
