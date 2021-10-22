//! This module contains functions to serve per-tenant background processes,
//! such as checkpointer and GC
use crate::tenant_mgr;
use crate::tenant_mgr::TenantState;
use crate::CheckpointConfig;
use crate::PageServerConf;
use anyhow::Result;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;
use tracing::*;
use zenith_utils::zid::ZTenantId;

struct TenantHandleEntry {
    checkpointer_handle: Option<JoinHandle<()>>,
    gc_handle: Option<JoinHandle<()>>,
}

// Preserve handles to wait for thread completion
// at shutdown
lazy_static! {
    static ref TENANT_HANDLES: Mutex<HashMap<ZTenantId, TenantHandleEntry>> =
        Mutex::new(HashMap::new());
}

pub fn start_tenant_threads(conf: &'static PageServerConf, tenantid: ZTenantId) {
    //ensure that old threads are stopeed
    wait_for_tenant_threads_to_stop(tenantid);

    let checkpointer_handle = std::thread::Builder::new()
        .name("Checkpointer thread".into())
        .spawn(move || {
            checkpoint_loop(tenantid, conf).expect("Checkpointer thread died");
        })
        .ok();

    let gc_handle = std::thread::Builder::new()
        .name("GC thread".into())
        .spawn(move || {
            gc_loop(tenantid, conf).expect("GC thread died");
        })
        .ok();

    // TODO handle thread errors if any

    let mut handles = TENANT_HANDLES.lock().unwrap();
    let h = TenantHandleEntry {
        checkpointer_handle,
        gc_handle,
    };

    handles.insert(tenantid, h);
}

pub fn wait_for_tenant_threads_to_stop(tenantid: ZTenantId) {
    let mut handles = TENANT_HANDLES.lock().unwrap();
    if let Some(h) = handles.get_mut(&tenantid) {
        h.checkpointer_handle.take().map(JoinHandle::join);
        trace!("checkpointer for tenant {} has stopped", tenantid);
        h.gc_handle.take().map(JoinHandle::join);
        trace!("gc for tenant {} has stopped", tenantid);
    }
    handles.remove(&tenantid);
}

///
/// Checkpointer thread's main loop
///
fn checkpoint_loop(tenantid: ZTenantId, conf: &'static PageServerConf) -> Result<()> {
    loop {
        if tenant_mgr::get_tenant_state(tenantid) != TenantState::Active {
            break;
        }

        std::thread::sleep(conf.checkpoint_period);
        trace!("checkpointer thread for tenant {} waking up", tenantid);

        // checkpoint timelines that have accumulated more than CHECKPOINT_DISTANCE
        // bytes of WAL since last checkpoint.
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        repo.checkpoint_iteration(CheckpointConfig::Distance(conf.checkpoint_distance))?;
    }

    trace!(
        "checkpointer thread stopped for tenant {} state is {}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}

///
/// GC thread's main loop
///
fn gc_loop(tenantid: ZTenantId, conf: &'static PageServerConf) -> Result<()> {
    loop {
        if tenant_mgr::get_tenant_state(tenantid) != TenantState::Active {
            break;
        }

        trace!("gc thread for tenant {} waking up", tenantid);

        // Garbage collect old files that are not needed for PITR anymore
        if conf.gc_horizon > 0 {
            let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
            repo.gc_iteration(None, conf.gc_horizon, false).unwrap();
        }

        // TODO Write it in more adequate way using
        // condvar.wait_timeout() or something
        let mut sleep_time = conf.gc_period.as_secs();
        while sleep_time > 0 && tenant_mgr::get_tenant_state(tenantid) == TenantState::Active {
            sleep_time -= 1;
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    trace!(
        "GC thread stopped for tenant {} state is {}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}
