pub mod endpoint;
pub mod error;
pub mod failpoints;
pub mod json;
pub mod request;
pub mod server;
pub mod tls_certs;

extern crate hyper0 as hyper;

/// Current fast way to apply simple http routing in various Neon binaries.
/// Re-exported for sake of uniform approach, that could be later replaced with better alternatives, if needed.
pub use routerify::{RequestServiceBuilder, RouterBuilder, RouterService, ext::RequestExt};
