use utils::id::TenantId;

use utils::generation::Generation;

use utils::id::TimelineId;

use std::collections::HashMap;

use utils::lsn::AtomicLsn;

use std::sync::Arc;

use utils::lsn::Lsn;

pub(crate) struct PendingLsn {
    pub(crate) projected: Lsn,
    pub(crate) result_slot: Arc<AtomicLsn>,
}

pub(crate) struct TenantLsnState {
    pub(crate) timelines: HashMap<TimelineId, PendingLsn>,

    // In what generation was the most recent update proposed?
    pub(crate) generation: Generation,
}

pub(crate) struct VisibleLsnUpdates {
    pub(crate) tenants: HashMap<TenantId, TenantLsnState>,
}

impl VisibleLsnUpdates {
    pub(crate) fn new() -> Self {
        Self {
            tenants: HashMap::new(),
        }
    }
}

impl std::fmt::Debug for VisibleLsnUpdates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VisibleLsnUpdates({} tenants)", self.tenants.len())
    }
}
