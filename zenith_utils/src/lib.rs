//! zenith_utils is intended to be a place to put code that is shared
//! between other crates in this repository.

/// `Lsn` type implements common tasks on Log Sequence Numbers
pub mod lsn;
/// SeqWait allows waiting for a future sequence number to arrive
pub mod seqwait;
