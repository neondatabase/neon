//! All binaries have the body of their main() defined here, so that the code
//! is also covered by code style configs in lib.rs and the unused-code check is
//! more effective when practically all modules are private to the lib.

pub mod local_proxy;
pub mod pg_sni_router;
pub mod proxy;
