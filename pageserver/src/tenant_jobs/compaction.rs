use once_cell::sync::OnceCell;
use utils::zid::ZTenantId;
use crate::repository::Repository;

use crate::tenant_mgr::{self, TenantState};

use super::worker::{Job, Pool};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompactionJob {
    pub tenant: ZTenantId,
}

impl Job for CompactionJob {
    type ErrorType = anyhow::Error;

    fn run(&self) -> Result<(), Self::ErrorType> {
        // TODO GC has the same code too
        if !matches!(tenant_mgr::get_tenant_state(self.tenant), Some(TenantState::Active)) {
            // TODO Maybe record this as "didn't run"?
            return Ok(());
        }

        let repo = tenant_mgr::get_repository_for_tenant(self.tenant)?;
        repo.compaction_iteration()?;

        Ok(())
    }
}

pub static COMPACTION_SCHEDULER: OnceCell<Pool<CompactionJob>> = OnceCell::new();

// TODO init pool with compaction interval
// TODO spawn 20 worker threads
