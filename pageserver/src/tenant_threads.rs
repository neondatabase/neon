//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC
use crate::repository::Repository;
use crate::tenant_mgr;
use crate::tenant_mgr::TenantState;
use anyhow::Result;
use tokio::task::JoinError;
use std::time::Duration;
use tracing::*;
use utils::zid::ZTenantId;

///
/// Compaction thread's main loop
///
pub fn compact_loop(tenantid: ZTenantId) -> Result<()> {
    if let Err(err) = compact_loop_ext(tenantid) {
        error!("compact loop terminated with error: {:?}", err);
        Err(err)
    } else {
        Ok(())
    }
}

fn compact_loop_ext(tenantid: ZTenantId) -> Result<()> {
    loop {
        if tenant_mgr::get_tenant_state(tenantid) != Some(TenantState::Active) {
            break;
        }
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        let compaction_period = repo.get_compaction_period();

        std::thread::sleep(compaction_period);
        trace!("compaction thread for tenant {} waking up", tenantid);

        // Compact timelines
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        repo.compaction_iteration()?;
    }

    trace!(
        "compaction thread stopped for tenant {} state is {:?}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}

///
/// GC thread's main loop
///
pub async fn gc_loop(tenantid: ZTenantId) -> Result<()> {
    loop {
        if tenant_mgr::get_tenant_state(tenantid) != Some(TenantState::Active) {
            break;
        }

        trace!("gc loop for tenant {} waking up", tenantid);
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        let gc_period = repo.get_gc_period();
        let gc_horizon = repo.get_gc_horizon();

        // Garbage collect old files that are not needed for PITR anymore
        if gc_horizon > 0 {
            let gc_result = tokio::task::spawn_blocking(move || {
                repo.gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)
            }).await;

            match gc_result {
                Ok(Ok(gc_result)) => {
                    // Gc success, do nothing
                }
                Ok(Err(e)) => {
                    error!("Gc failed: {}", e);
                    // TODO maybe also don't reschedule on error?
                }
                Err(e) => {
                    error!("Gc failed: {}", e);
                    // TODO maybe also don't reschedule on error?
                }
            }
        }

        tokio::time::sleep(gc_period);
    }
    trace!(
        "GC loop stopped for tenant {} state is {:?}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}
