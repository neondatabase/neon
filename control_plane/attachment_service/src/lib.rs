use serde::{Deserialize, Serialize};
use utils::seqwait::MonotonicCounter;

mod compute_hook;
pub mod http;
mod node;
pub mod persistence;
mod reconciler;
mod scheduler;
mod schema;
pub mod service;
mod tenant_state;

#[derive(Clone, Serialize, Deserialize)]
enum PlacementPolicy {
    /// Cheapest way to attach a tenant: just one pageserver, no secondary
    Single,
    /// Production-ready way to attach a tenant: one attached pageserver and
    /// some number of secondaries.
    Double(usize),
    /// Do not attach to any pageservers
    Detached,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
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
