use serde::{Deserialize, Serialize};
use utils::seqwait::MonotonicCounter;

mod auth;
mod compute_hook;
pub mod http;
pub mod metrics;
mod node;
pub mod persistence;
mod reconciler;
mod scheduler;
mod schema;
pub mod service;
mod tenant_state;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
enum PlacementPolicy {
    /// Cheapest way to attach a tenant: just one pageserver, no secondary
    Single,
    /// Production-ready way to attach a tenant: one attached pageserver and
    /// some number of secondaries.
    Double(usize),
    /// Create one secondary mode locations. This is useful when onboarding
    /// a tenant, or for an idle tenant that we might want to bring online quickly.
    Secondary,

    /// Do not attach to any pageservers.  This is appropriate for tenants that
    /// have been idle for a long time, where we do not mind some delay in making
    /// them available in future.
    Detached,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize)]
struct Sequence(u64);

impl Sequence {
    fn initial() -> Self {
        Self(0)
    }
}

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl MonotonicCounter<Sequence> for Sequence {
    fn cnt_advance(&mut self, v: Sequence) {
        assert!(*self <= v);
        *self = v;
    }
    fn cnt_value(&self) -> Sequence {
        *self
    }
}

impl Sequence {
    fn next(&self) -> Sequence {
        Sequence(self.0 + 1)
    }
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        PlacementPolicy::Double(1)
    }
}
