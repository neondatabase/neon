//! This module is for WAL craft to test with postgres_ffi. Should not import any thing in normal usage.

pub use super::PG_MAJORVERSION;
pub use super::xlog_utils::*;
pub use super::bindings::*;
pub use crate::WAL_SEGMENT_SIZE;
