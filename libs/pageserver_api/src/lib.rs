use const_format::formatcp;

/// Public API types
pub mod models;

pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
