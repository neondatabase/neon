//! Local control plane.
//!
//! Can start, configure and stop postgres instances running as a local processes.
//!
//! Intended to be used in integration tests and in CLI tools for
//! local installations.
#![deny(clippy::undocumented_unsafe_blocks)]

mod background_process;
pub mod broker;
pub mod endpoint;
pub mod local_env;
pub mod pageserver;
pub mod postgresql_conf;
pub mod safekeeper;
pub mod storage_controller;
