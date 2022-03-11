//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC
use crate::repository::Repository;
use crate::tenant_mgr;
use crate::tenant_mgr::TenantState;
use anyhow::Result;
use std::time::Duration;
use tracing::*;
use zenith_utils::zid::ZTenantId;

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
        let tenant_conf = repo.get_tenant_conf();

        std::thread::sleep(tenant_conf.compaction_period);
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
pub fn gc_loop(tenantid: ZTenantId) -> Result<()> {
    loop {
        if tenant_mgr::get_tenant_state(tenantid) != Some(TenantState::Active) {
            break;
        }

        trace!("gc thread for tenant {} waking up", tenantid);
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        let tenant_conf = repo.get_tenant_conf();

        // Garbage collect old files that are not needed for PITR anymore
        if tenant_conf.gc_horizon > 0 {
            repo.gc_iteration(
                None,
                tenant_conf.gc_horizon,
                tenant_conf.pitr_interval,
                false,
            )?;
        }

        // TODO Write it in more adequate way using
        // condvar.wait_timeout() or something
        let mut sleep_time = tenant_conf.gc_period.as_secs();
        while sleep_time > 0 && tenant_mgr::get_tenant_state(tenantid) == Some(TenantState::Active)
        {
            sleep_time -= 1;
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    trace!(
        "GC thread stopped for tenant {} state is {:?}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}
