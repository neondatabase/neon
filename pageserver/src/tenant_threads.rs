//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::collections::HashMap;
use std::ops::ControlFlow;

use crate::repository::Repository;
use crate::tenant_mgr::TenantState;
use crate::thread_mgr::ThreadKind;
use crate::{tenant_mgr, thread_mgr};
use anyhow::{self, Context};
use once_cell::sync::OnceCell;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::*;
use utils::zid::ZTenantId;

// TODO metrics

///
/// Compaction task's main loop
///
async fn compaction_loop(
    tenantid: ZTenantId,
    mut cancel: watch::Receiver<()>,
) -> anyhow::Result<()> {
    loop {
        trace!("waking up");

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
                tokio::select! {
                    _ = cancel.changed() => {
                        trace!("received cancellation request");
                        break;
                    }
                    _ = tokio::time::sleep(period) => {},
                }
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
        "compaction loop stopped. State is {:?}",
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}

static START_GC_LOOP: OnceCell<mpsc::Sender<ZTenantId>> = OnceCell::new();
static START_COMPACTION_LOOP: OnceCell<mpsc::Sender<ZTenantId>> = OnceCell::new();

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

    // TODO this is getting repetitive
    let mut gc_loops = HashMap::<ZTenantId, watch::Sender<()>>::new();
    let mut compaction_loops = HashMap::<ZTenantId, watch::Sender<()>>::new();

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
                            // TODO do this from thread_mgr? Extend it to work with tasks?
                            for (_, cancel) in gc_loops.drain() {
                                cancel.send(()).ok();
                            }
                            for (_, cancel) in compaction_loops.drain() {
                                cancel.send(()).ok();
                            }
                            // TODO wait for tasks to die?
                            break;
                        },
                        tenantid = gc_recv.recv() => {
                            let tenantid = tenantid.expect("Gc task channel closed unexpectedly");

                            // Spawn new task, request cancellation of the old one if exists
                            let (cancel_send, cancel_recv) = watch::channel(());
                            let _handle = tokio::spawn(gc_loop(tenantid, cancel_recv))
                                .instrument(trace_span!("gc loop", tenant = %tenantid));
                            if let Some(old_cancel_send) = gc_loops.insert(tenantid, cancel_send) {
                                old_cancel_send.send(()).ok();
                            }
                        },
                        tenantid = compaction_recv.recv() => {
                            let tenantid = tenantid.expect("Compaction task channel closed unexpectedly");

                            // Spawn new task, request cancellation of the old one if exists
                            let (cancel_send, cancel_recv) = watch::channel(());
                            let _handle = tokio::spawn(compaction_loop(tenantid, cancel_recv))
                                .instrument(trace_span!("compaction loop", tenant = %tenantid));
                            if let Some(old_cancel_send) = compaction_loops.insert(tenantid, cancel_send) {
                                old_cancel_send.send(()).ok();
                            }
                        },
                        // TODO await return values? Report errors?
                    }
                }
            });
            Ok(())
        },
    )?;

    Ok(())
}

///
/// GC task's main loop
///
async fn gc_loop(tenantid: ZTenantId, mut cancel: watch::Receiver<()>) -> anyhow::Result<()> {
    loop {
        trace!("waking up");

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
                tokio::select! {
                    _ = cancel.changed() => {
                        trace!("received cancellation request");
                        break;
                    }
                    _ = tokio::time::sleep(period) => {},
                }
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
        "GC loop stopped. State is {:?}",
        tenant_mgr::get_tenant_state(tenantid)
    );
    Ok(())
}
