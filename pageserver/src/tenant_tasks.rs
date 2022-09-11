//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::time::Duration;

use crate::metrics::TENANT_TASK_EVENTS;
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant_mgr;
use crate::tenant_mgr::TenantState;
use tracing::*;
use utils::zid::ZTenantId;

pub fn start_background_loops(tenant_id: ZTenantId) {
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        Some(tenant_id),
        None,
        &format!("compactor for tenant {tenant_id}"),
        false,
        compaction_loop(tenant_id),
    );
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        None,
        &format!("garbage collector for tenant {tenant_id}"),
        false,
        gc_loop(tenant_id),
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(tenant_id: ZTenantId) -> anyhow::Result<()> {
    info!("starting compaction loop for {tenant_id}");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    let result = async {
        loop {
            trace!("waking up");

            // Run blocking part of the task

            // Break if tenant is not active
            if tenant_mgr::get_tenant_state(tenant_id) != Some(TenantState::Active) {
                break Ok(());
            }
            // This should not fail. If someone started us, it means that the tenant exists.
            // And before you remove a tenant, you have to wait until all the associated tasks
            // exit.
            let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;

            // Run compaction
            let mut sleep_duration = repo.get_compaction_period();
            if let Err(e) = repo.compaction_iteration() {
                error!("Compaction failed, retrying: {}", e);
                sleep_duration = Duration::from_secs(2)
            }

            // Sleep
            tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    trace!("received cancellation request");
                    break Ok(());
                },
                _ = tokio::time::sleep(sleep_duration) => {},
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();

    info!(
        "compaction loop stopped. State is {:?}",
        tenant_mgr::get_tenant_state(tenant_id)
    );
    result
}

///
/// GC task's main loop
///
async fn gc_loop(tenant_id: ZTenantId) -> anyhow::Result<()> {
    info!("starting gc loop for {tenant_id}");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    let result = async {
        loop {
            trace!("waking up");

            // Break if tenant is not active
            if tenant_mgr::get_tenant_state(tenant_id) != Some(TenantState::Active) {
                break Ok(());
            }
            // This should not fail. If someone started us, it means that the tenant exists.
            // And before you remove a tenant, you have to wait until all the associated tasks
            // exit.
            let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;

            // Run gc
            let gc_period = repo.get_gc_period();
            let gc_horizon = repo.get_gc_horizon();
            let mut sleep_duration = gc_period;
            if gc_horizon > 0 {
                if let Err(e) = repo.gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)
                {
                    error!("Gc failed, retrying: {}", e);
                    sleep_duration = Duration::from_secs(2)
                }
            }

            // Sleep
            tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    trace!("received cancellation request");
                    break Ok(());
                },
                _ = tokio::time::sleep(sleep_duration) => {},
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
    info!(
        "GC loop stopped. State is {:?}",
        tenant_mgr::get_tenant_state(tenant_id)
    );
    result
}
