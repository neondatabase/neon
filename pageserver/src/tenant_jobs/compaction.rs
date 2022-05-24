use std::{ops::Add, time::Instant};

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

    fn run(&self) -> Result<Option<Instant>, Self::ErrorType> {
        // Don't reschedule job if tenant isn't active
        if !matches!(tenant_mgr::get_tenant_state(self.tenant), Some(TenantState::Active)) {
            return Ok(None);
        }

        let repo = tenant_mgr::get_repository_for_tenant(self.tenant)?;
        repo.compaction_iteration()?;

        Ok(Some(Instant::now().add(repo.get_compaction_period())))
    }
}

pub static COMPACTION_SCHEDULER: OnceCell<Pool<CompactionJob>> = OnceCell::new();

// TODO spawn 20 worker threads
// TODO add tasks when tenant activates
