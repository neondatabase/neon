//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::ops::ControlFlow;

use crate::repository::Repository;
use crate::tenant_mgr::TenantState;
use crate::thread_mgr::ThreadKind;
use crate::{tenant_mgr, thread_mgr};
use anyhow::{self, Context};
use once_cell::sync::OnceCell;
use tokio::sync::mpsc::{self, Sender};
use tracing::*;
use utils::zid::ZTenantId;

///
/// Compaction task's main loop
///
async fn compaction_loop(tenantid: ZTenantId) -> anyhow::Result<()> {
    if let Err(err) = compaction_loop_ext(tenantid).await {
        error!("compact loop terminated with error: {:?}", err);
        Err(err)
    } else {
        Ok(())
    }
}

async fn compaction_loop_ext(tenantid: ZTenantId) -> anyhow::Result<()> {
    loop {
        trace!("compaction loop for tenant {} waking up", tenantid);

        // Run blocking part of the task
        let period: Result<Result<_, anyhow::Error>, _> = tokio::task::spawn_blocking(move || {
            if tenant_mgr::get_tenant_state(tenantid) != Some(TenantState::Active) {
                return Ok(ControlFlow::Break(()));
            }
            let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
            let compaction_period = repo.get_compaction_period();
            repo.compaction_iteration()?;
            Ok(ControlFlow::Continue(compaction_period))
        })
        .await;

        // Handle result
        match period {
            Ok(Ok(ControlFlow::Continue(period))) => {
                tokio::time::sleep(period).await;
            }
            Ok(Ok(ControlFlow::Break(()))) => {
                break;
            }
            Ok(Err(e)) => {
                anyhow::bail!("Compaction failed: {}", e);
            }
            Err(e) => {
                anyhow::bail!("Compaction join error: {}", e);
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

/// Spawn a task that will periodically schedule garbage collection until
/// the tenant becomes inactive. This should be called on tenant
/// activation.
pub fn start_gc_loop(tenantid: ZTenantId) -> anyhow::Result<()> {
    START_GC_LOOP
        .get()
        .context("Failed to get START_GC_LOOP")?
        .blocking_send(tenantid)
        .map_err(|e| anyhow::anyhow!(e))
        .context("Failed to send to START_GC_LOOP channel")?;
    Ok(())
}

/// Spawn a task that will periodically schedule compaction until
/// the tenant becomes inactive. This should be called on tenant
/// activation.
pub fn start_compaction_loop(tenantid: ZTenantId) -> anyhow::Result<()> {
    START_COMPACTION_LOOP
        .get()
        .context("failed to get START_COMPACTION_LOOP")?
        .blocking_send(tenantid)
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to send to START_COMPACTION_LOOP")?;
    Ok(())
}

/// Spawn the TenantTaskManager
/// This needs to be called before start_gc_loop or start_compaction_loop
pub fn init_tenant_task_pool() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("tenant-task-worker")
        .worker_threads(40) // Way more than necessary
        .max_blocking_threads(100) // Way more than necessary
        .enable_all()
        .build()?;

    let (gc_send, mut gc_recv) = mpsc::channel::<ZTenantId>(100);
    START_GC_LOOP
        .set(gc_send)
        .expect("Failed to set START_GC_LOOP");

    let (compaction_send, mut compaction_recv) = mpsc::channel::<ZTenantId>(100);
    START_COMPACTION_LOOP
        .set(compaction_send)
        .expect("Failed to set START_COMPACTION_LOOP");

    thread_mgr::spawn(
        ThreadKind::TenantTaskManager,
        None,
        None,
        "Tenant task manager main thread",
        true,
        move || {
            runtime.block_on(async move {
                loop {
                    tokio::select! {
                        _ = thread_mgr::shutdown_watcher() => {
                            // TODO cancel all running tasks
                            break
                        },
                        tenantid = gc_recv.recv() => {
                            let tenantid = tenantid.expect("Gc task channel closed unexpectedly");
                            // TODO cancel existing loop, if any.
                            tokio::spawn(gc_loop(tenantid));
                        },
                        tenantid = compaction_recv.recv() => {
                            let tenantid = tenantid.expect("Compaction task channel closed unexpectedly");
                            // TODO cancel existing loop, if any.
                            tokio::spawn(compaction_loop(tenantid));
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
async fn gc_loop(tenantid: ZTenantId) -> anyhow::Result<()> {
    loop {
        trace!("gc loop for tenant {} waking up", tenantid);

        // Run blocking part of the task
        let period: Result<Result<_, anyhow::Error>, _> = tokio::task::spawn_blocking(move || {
            if tenant_mgr::get_tenant_state(tenantid) != Some(TenantState::Active) {
                return Ok(ControlFlow::Break(()));
            }
            let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
            let gc_period = repo.get_gc_period();
            let gc_horizon = repo.get_gc_horizon();

            if gc_horizon > 0 {
                repo.gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)?;
            }

            Ok(ControlFlow::Continue(gc_period))
        })
        .await;

        // Handle result
        match period {
            Ok(Ok(ControlFlow::Continue(period))) => {
                tokio::time::sleep(period).await;
            }
            Ok(Ok(ControlFlow::Break(()))) => {
                break;
            }
            Ok(Err(e)) => {
                anyhow::bail!("Gc failed: {}", e);
            }
            Err(e) => {
                anyhow::bail!("Gc join error: {}", e);
            }
        }
    }
    trace!(
        "GC loop stopped for tenant {} state is {:?}",
        tenantid,
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}
