use std::{ops::Add, time::Instant};

use crate::{
    repository::Repository,
    tenant_mgr::{self, TenantState},
};
use once_cell::sync::OnceCell;
use utils::zid::ZTenantId;

use super::{job::Job, worker::Pool};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GcJob {
    pub tenant: ZTenantId,
}

impl Job for GcJob {
    type ErrorType = anyhow::Error;

    fn run(&self) -> Result<Option<Instant>, Self::ErrorType> {
        // Don't reschedule job if tenant isn't active
        if !matches!(
            tenant_mgr::get_tenant_state(self.tenant),
            Some(TenantState::Active)
        ) {
            return Ok(None);
        }

        let repo = tenant_mgr::get_repository_for_tenant(self.tenant)?;
        let gc_horizon = repo.get_gc_horizon();
        if gc_horizon > 0 {
            repo.gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)?;
        }

        Ok(Some(Instant::now().add(repo.get_gc_period())))
    }
}

pub static GC_POOL: OnceCell<Pool<GcJob>> = OnceCell::new();
