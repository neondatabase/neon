use once_cell::sync::OnceCell;
use utils::zid::ZTenantId;
use tracing::*;
use crate::repository::Repository;

use crate::tenant_mgr::{self, TenantState};

use super::chore::{Job, SimpleScheduler};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcJob {
    pub tenant: ZTenantId,
}

impl Job for GcJob {
    type ErrorType = anyhow::Error;

    fn run(&self) -> Result<(), Self::ErrorType> {
        // TODO why not kill the chore when tenant is not active?
        if !matches!(tenant_mgr::get_tenant_state(self.tenant), Some(TenantState::Active(_, _))) {
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

pub static GC_SCHEDULER: OnceCell<SimpleScheduler<GcJob>> = OnceCell::new();
