//! Various stuff for dealing with the Neon Console.
//! Later we might move some API wrappers here.

/// Payloads used in the console's APIs.
pub mod messages;

/// Wrappers for console APIs and their mocks.
pub mod provider;
pub(crate) use provider::{errors, Api, AuthSecret, CachedNodeInfo, NodeInfo};

/// Various cache-related types.
pub mod caches {
    pub use super::provider::ApiCaches;
    pub(crate) use super::provider::NodeInfoCache;
}

/// Various cache-related types.
pub mod locks {
    pub use super::provider::ApiLocks;
}

/// Console's management API.
pub mod mgmt;
