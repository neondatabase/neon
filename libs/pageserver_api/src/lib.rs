#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]
use const_format::formatcp;

/// Public API types
pub mod control_api;
pub mod key;
pub mod keyspace;
pub mod models;
pub mod reltag;
pub mod shard;

pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
