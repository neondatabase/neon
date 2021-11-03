//! zenith_utils is intended to be a place to put code that is shared
//! between other crates in this repository.

#![allow(clippy::manual_range_contains)]

/// `Lsn` type implements common tasks on Log Sequence Numbers
pub mod lsn;
/// SeqWait allows waiting for a future sequence number to arrive
pub mod seqwait;

/// append only ordered map implemented with a Vec
pub mod vec_map;

// Async version of SeqWait. Currently unused.
// pub mod seqwait_async;

pub mod bin_ser;
pub mod postgres_backend;
pub mod pq_proto;

// dealing with connstring parsing and handy access to it's parts
pub mod connstring;

// helper functions for creating and fsyncing directories/trees
pub mod crashsafe_dir;

// common authentication routines
pub mod auth;

// utility functions and helper traits for unified unique id generation/serialization etc.
pub mod zid;
// http endpoint utils
pub mod http;

// socket splitting utils
pub mod sock_split;

// common log initialisation routine
pub mod logging;

// Misc
pub mod accum;

// Utility for binding TcpListeners with proper socket options.
pub mod tcp_listener;

// Call fsync for many files at once.
pub mod batch_fsync;
