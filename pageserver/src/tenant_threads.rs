//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use crate::repository::Repository;
use crate::tenant_mgr::TenantState;
use crate::thread_mgr::ThreadKind;
use crate::{tenant_mgr, thread_mgr};
use anyhow::Result;
use once_cell::sync::OnceCell;
use tokio::sync::mpsc::{self, Sender};
use tracing::*;
use utils::zid::ZTenantId;

///
/// Compaction task's main loop
///
async fn compaction_loop(tenantid: ZTenantId) -> Result<()> {
    if let Err(err) = compaction_loop_ext(tenantid).await {
        error!("compact loop terminated with error: {:?}", err);
        Err(err)
    } else {
        Ok(())
    }
}

async fn compaction_loop_ext(tenantid: ZTenantId) -> Result<()> {
    loop {
        if tenant_mgr::get_tenant_state(tenantid) != Some(TenantState::Active) {
            break;
        }
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        let compaction_period = repo.get_compaction_period();

        tokio::time::sleep(compaction_period).await;
        trace!("compaction loop for tenant {} waking up", tenantid);

        // Compact timelines
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        let compaction_result =
            tokio::task::spawn_blocking(move || repo.compaction_iteration()).await;
        match compaction_result {
            Ok(Ok(())) => {
                // success, do nothing
            }
            Ok(Err(e)) => {
                anyhow::bail!(e.context("Compaction failed"));
            }
            Err(e) => {
                anyhow::bail!("Compaction join error {}", e);
            }
        }
    }

    trace!(
        "compaction loop stopped for tenant {} state is {:?}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}

static START_GC_LOOP: OnceCell<Sender<ZTenantId>> = OnceCell::new();
static START_COMPACTION_LOOP: OnceCell<Sender<ZTenantId>> = OnceCell::new();

pub fn start_gc_loop(tenantid: ZTenantId) -> Result<()> {
    START_GC_LOOP
        .get()
        .unwrap()
        .blocking_send(tenantid)
        .unwrap();
    Ok(())
}

pub fn start_compaction_loop(tenantid: ZTenantId) -> Result<()> {
    START_COMPACTION_LOOP
        .get()
        .unwrap()
        .blocking_send(tenantid)
        .unwrap();
    Ok(())
}

/// Spawn the TenantTaskManager
/// This needs to be called before start_gc_loop or start_compaction_loop
pub fn init_tenant_task_pool() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("tenant-task-worker")
        .worker_threads(40)  // Way more than necessary
        .max_blocking_threads(100)  // Way more than necessary
        .enable_all()
        .build()?;

    let (gc_send, mut gc_recv) = mpsc::channel::<ZTenantId>(100);
    START_GC_LOOP.set(gc_send).unwrap();

    let (compaction_send, mut compaction_recv) = mpsc::channel::<ZTenantId>(100);
    START_COMPACTION_LOOP.set(compaction_send).unwrap();

    thread_mgr::spawn(
        ThreadKind::TenantTaskManager,
        None,
        None,
        "WAL receiver manager main thread",
        true,
        move || {
            runtime.block_on(async move {
                loop {
                    tokio::select! {
                        _ = thread_mgr::shutdown_watcher() => break,
                        // TODO don't spawn if already running
                        tenantid = gc_recv.recv() => {
                            tokio::spawn(gc_loop(tenantid.unwrap()));
                        },
                        tenantid = compaction_recv.recv() => {
                            tokio::spawn(compaction_loop(tenantid.unwrap()));
                        },
                    }
                }
            });
            Ok(())
        },
    )?;

    Ok(())
}

///
/// GC thread's main loop
///
async fn gc_loop(tenantid: ZTenantId) -> Result<()> {
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
            })
            .await;

            match gc_result {
                Ok(Ok(_gc_result)) => {
                    // Gc success, do nothing
                }
                Ok(Err(e)) => {
                    anyhow::bail!(e.context("Gc failed"));
                }
                Err(e) => {
                    anyhow::bail!("Gc join error {}", e);
                }
            }
        }

        tokio::time::sleep(gc_period).await;
    }
    trace!(
        "GC loop stopped for tenant {} state is {:?}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}
