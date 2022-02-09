pub mod endpoint;
pub mod error;
pub mod json;
pub mod request;

/// Current fast way to applly simple http routing in varuious Zenith binaries.
/// Reexported for sake of uniform approach, that could be later replaced with better alternatives, if needed.
pub use routerify::{ext::RequestExt, RouterBuilder};
