use once_cell::sync::OnceCell;
use utils::zid::ZTenantId;
use tracing::*;
use crate::repository::Repository;

use crate::tenant_mgr::{self, TenantState};

use super::chore::{Job, SimpleScheduler};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionJob {
    pub tenant: ZTenantId,
}

impl Job for CompactionJob {
    type ErrorType = anyhow::Error;

    fn run(&self) -> Result<(), Self::ErrorType> {
        // TODO why not kill the chore when tenant is not active?
        // TODO GC has the same code too
        if !matches!(tenant_mgr::get_tenant_state(self.tenant), Some(TenantState::Active(_, _))) {
            return Ok(());
        }

        let repo = tenant_mgr::get_repository_for_tenant(self.tenant)?;
        repo.compaction_iteration()?;

        Ok(())
    }
}

pub static COMPACTION_SCHEDULER: OnceCell<SimpleScheduler<CompactionJob>> = OnceCell::new();
