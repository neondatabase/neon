///! Various stuff for dealing with the Neon Console.
///! Later we might move some API wrappers here.

/// Payloads used in the console's APIs.
pub mod messages;

/// Wrappers for console APIs and their mocks.
pub mod provider;
pub use provider::{errors, Api, ConsoleReqExtra};
pub use provider::{AuthInfo, NodeInfo};
pub use provider::{CachedAuthInfo, CachedNodeInfo};

/// Various cache-related types.
pub mod caches {
    pub use super::provider::{ApiCaches, AuthInfoCache, AuthInfoCacheKey, NodeInfoCache};
}

/// Console's management API.
pub mod mgmt;

/// Console's notification bus.
pub mod notifications;
