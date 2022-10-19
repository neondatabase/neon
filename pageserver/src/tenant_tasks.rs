//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::sync::Arc;
use std::time::Duration;

use crate::metrics::TENANT_TASK_EVENTS;
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::Tenant;
use tracing::*;

pub fn start_background_loops(tenant: &Arc<Tenant>) {
    let tenant_id = tenant.tenant_id();
    let tenant_clone = Arc::clone(tenant);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        Some(tenant_id),
        None,
        &format!("compactor for tenant {tenant_id}"),
        false,
        async move {
            compaction_loop(tenant_clone)
                .instrument(info_span!("compaction_loop", tenant_id = %tenant_id))
                .await;
            Ok(())
        },
    );
    let tenant_clone = Arc::clone(tenant);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        None,
        &format!("garbage collector for tenant {tenant_id}"),
        false,
        async move {
            gc_loop(tenant_clone)
                .instrument(info_span!("gc_loop", tenant_id = %tenant_id))
                .await;
            Ok(())
        },
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(tenant: Arc<Tenant>) {
    let wait_duration = Duration::from_secs(2);
    info!("starting");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        loop {
            trace!("waking up");

            // Run compaction
            let mut sleep_duration = tenant.get_compaction_period();
            if let Err(e) = tenant.compaction_iteration().await {
                sleep_duration = wait_duration;
                error!("Compaction failed, retrying in {:?}: {e:#}", sleep_duration);
                #[cfg(feature = "testing")]
                std::process::abort();
            }

            // Sleep
            tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    info!("received cancellation request during idling");
                    break ;
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
async fn gc_loop(tenant: Arc<Tenant>) {
    let wait_duration = Duration::from_secs(2);
    info!("starting");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        loop {
            trace!("waking up");

            // Run gc
            let gc_period = tenant.get_gc_period();
            let gc_horizon = tenant.get_gc_horizon();
            let mut sleep_duration = gc_period;
            if gc_horizon > 0 {
                if let Err(e) = tenant
                    .gc_iteration(None, gc_horizon, tenant.get_pitr_interval(), false)
                    .await
                {
                    sleep_duration = wait_duration;
                    error!("Gc failed, retrying in {:?}: {e:#}", sleep_duration);
                    #[cfg(feature = "testing")]
                    std::process::abort();
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
