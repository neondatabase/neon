pub mod endpoint;
pub mod error;
pub mod json;
pub mod request;

/// Current fast way to apply simple http routing in various Neon binaries.
/// Re-exported for sake of uniform approach, that could be later replaced with better alternatives, if needed.
pub use routerify::{ext::RequestExt, RouterBuilder, RouterService};
