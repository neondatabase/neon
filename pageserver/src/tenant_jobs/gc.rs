use once_cell::sync::OnceCell;
use utils::zid::ZTenantId;
use crate::{repository::Repository, tenant_mgr::{self, TenantState}};

use super::worker::{Job, Pool};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GcJob {
    pub tenant: ZTenantId,
}

impl Job for GcJob {
    type ErrorType = anyhow::Error;

    fn run(&self) -> Result<(), Self::ErrorType> {
        if !matches!(tenant_mgr::get_tenant_state(self.tenant), Some(TenantState::Active)) {
            // TODO Maybe record this as "didn't run"?
            // TODO Maybe unschedule?
            return Ok(());
        }

        let repo = tenant_mgr::get_repository_for_tenant(self.tenant)?;
        let gc_horizon = repo.get_gc_horizon();
        if gc_horizon > 0 {
            repo.gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)?;
        }

        Ok(())
    }
}

pub static GC_POOL: OnceCell<Pool<GcJob>> = OnceCell::new();

// TODO spawn 20 worker threads
// TODO add tasks when tenant activates
