//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use crate::context::{DownloadBehavior, RequestContext};
use crate::metrics::TENANT_TASK_EVENTS;
use crate::task_mgr;
use crate::task_mgr::{TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::mgr;
use crate::tenant::{Tenant, TenantState};
use tracing::*;
use utils::id::TenantId;

pub fn start_background_loops(tenant_id: TenantId) {
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        Some(tenant_id),
        None,
        &format!("compactor for tenant {tenant_id}"),
        false,
        async move {
            compaction_loop(tenant_id)
                .instrument(info_span!("compaction_loop", tenant_id = %tenant_id))
                .await;
            Ok(())
        },
    );
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        None,
        &format!("garbage collector for tenant {tenant_id}"),
        false,
        async move {
            gc_loop(tenant_id)
                .instrument(info_span!("gc_loop", tenant_id = %tenant_id))
                .await;
            Ok(())
        },
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(tenant_id: TenantId) {
    let wait_duration = Duration::from_secs(2);
    info!("starting");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        let ctx = RequestContext::todo_child(TaskKind::Compaction, DownloadBehavior::Download);
        loop {
            trace!("waking up");

            let tenant = tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    info!("received cancellation request");
                return;
                },
                tenant_wait_result = wait_for_active_tenant(tenant_id, wait_duration) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(tenant) => tenant,
                },
        };

            let mut sleep_duration = tenant.get_compaction_period();
            if sleep_duration == Duration::ZERO {
                info!("automatic compaction is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                sleep_duration = Duration::from_secs(10);
            } else {
                // Run compaction
                if let Err(e) = tenant.compaction_iteration(&ctx).await {
                    sleep_duration = wait_duration;
                    error!("Compaction failed, retrying in {:?}: {e:?}", sleep_duration);
                }
            }

            // Sleep
            tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    info!("received cancellation request during idling");
                    break;
                },
                _ = tokio::time::sleep(sleep_duration) => {},
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();

    trace!("compaction loop stopped.");
}

///
/// GC task's main loop
///
async fn gc_loop(tenant_id: TenantId) {
    let wait_duration = Duration::from_secs(2);
    info!("starting");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        // GC might require downloading, to find the cutoff LSN that corresponds to the
        // cutoff specified as time.
        let ctx = RequestContext::todo_child(TaskKind::GarbageCollector, DownloadBehavior::Download);
        loop {
            trace!("waking up");

            let tenant = tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    info!("received cancellation request");
                    return;
                },
                tenant_wait_result = wait_for_active_tenant(tenant_id, wait_duration) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(tenant) => tenant,
                },
            };

            let gc_period = tenant.get_gc_period();
            let gc_horizon = tenant.get_gc_horizon();
            let mut sleep_duration = gc_period;
            if sleep_duration == Duration::ZERO {
                info!("automatic GC is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                sleep_duration = Duration::from_secs(10);
            } else {
                // Run gc
                if gc_horizon > 0 {
                    if let Err(e) = tenant.gc_iteration(None, gc_horizon, tenant.get_pitr_interval(), &ctx).await
                    {
                        sleep_duration = wait_duration;
                        error!("Gc failed, retrying in {:?}: {e:?}", sleep_duration);
                    }
                }
            }

            // Sleep
            tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    info!("received cancellation request during idling");
                    break;
                },
                _ = tokio::time::sleep(sleep_duration) => {},
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
    trace!("GC loop stopped.");
}

async fn wait_for_active_tenant(
    tenant_id: TenantId,
    wait: Duration,
) -> ControlFlow<(), Arc<Tenant>> {
    let tenant = loop {
        match mgr::get_tenant(tenant_id, false).await {
            Ok(tenant) => break tenant,
            Err(e) => {
                error!("Failed to get a tenant {tenant_id}: {e:#}");
                tokio::time::sleep(wait).await;
            }
        }
    };

    // if the tenant has a proper status already, no need to wait for anything
    if tenant.current_state() == TenantState::Active {
        ControlFlow::Continue(tenant)
    } else {
        let mut tenant_state_updates = tenant.subscribe_for_state_updates();
        loop {
            match tenant_state_updates.changed().await {
                Ok(()) => {
                    let new_state = *tenant_state_updates.borrow();
                    match new_state {
                        TenantState::Active => {
                            debug!("Tenant state changed to active, continuing the task loop");
                            return ControlFlow::Continue(tenant);
                        }
                        state => {
                            debug!("Not running the task loop, tenant is not active: {state:?}");
                            continue;
                        }
                    }
                }
                Err(_sender_dropped_error) => {
                    info!("Tenant dropped the state updates sender, quitting waiting for tenant and the task loop");
                    return ControlFlow::Break(());
                }
            }
        }
    }
}
