//! zenith_utils is intended to be a place to put code that is shared
//! between other crates in this repository.

/// `Lsn` type implements common tasks on Log Sequence Numbers
pub mod lsn;
/// SeqWait allows waiting for a future sequence number to arrive
pub mod seqwait;

// Async version of SeqWait. Currently unused.
// pub mod seqwait_async;

pub mod bin_ser;
pub mod http_endpoint;
pub mod postgres_backend;
pub mod pq_proto;

// dealing with connstring parsing and handy access to it's parts
pub mod connstring;
