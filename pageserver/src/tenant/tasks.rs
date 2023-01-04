//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::sync::Arc;
use std::time::Duration;

use crate::context::{DownloadBehavior, RequestContext, TaskKind};
use crate::metrics::TENANT_TASK_EVENTS;
use crate::task_mgr;
use crate::task_mgr::BACKGROUND_RUNTIME;
use crate::tenant::Tenant;
use tracing::*;

pub fn start_background_loops(tenant: &Arc<Tenant>) {
    let tenant_id = tenant.tenant_id;

    let tenant_clone = Arc::clone(tenant);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        &format!("compactor for tenant {tenant_id}"),
        false,
        async move {
            compaction_loop(&tenant_clone)
                .instrument(info_span!("compaction_loop", tenant_id = %tenant_id))
                .await;
            Ok(())
        },
    );
    let tenant_clone = Arc::clone(tenant);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        &format!("garbage collector for tenant {tenant_id}"),
        false,
        async move {
            gc_loop(&tenant_clone)
                .instrument(info_span!("gc_loop", tenant_id = %tenant_id))
                .await;
            Ok(())
        },
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(tenant: &Arc<Tenant>) {
    let wait_duration = Duration::from_secs(2);
    info!("starting");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        let top_cxt = RequestContext::new(TaskKind::Compaction, DownloadBehavior::Download);

        let tenant_cxt = match tenant.get_context(&top_cxt) {
            Ok(cxt) => cxt,
            Err(state) => {
                // This could happen if the tenant is detached or the pageserver is shut
                // down immediately after loading or attaching completed and the tenant
                // was activated. It seems unlikely enough in practice that we better print
                // a warning, as it could also be a bug.
                error!("Not running compaction loop, tenant is not active: {state:?}");
                return;
            }
        };
        loop {
            trace!("waking up");

            let mut sleep_duration = tenant.get_compaction_period();
            if sleep_duration == Duration::ZERO {
                info!("automatic compaction is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                sleep_duration = Duration::from_secs(10);
            } else {
                // Run compaction
                if let Err(e) = tenant.compaction_iteration(&tenant_cxt).await {
                    sleep_duration = wait_duration;
                    error!("Compaction failed, retrying in {:?}: {e:?}", sleep_duration);
                }
            }

            // Sleep
            tokio::select! {
                _ = tenant_cxt.cancelled() => {
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
async fn gc_loop(tenant: &Arc<Tenant>) {
    let wait_duration = Duration::from_secs(2);
    info!("starting");
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        // GC might require downloading, to find the cutoff LSN that corresponds to the
        // cutoff specified as time.
        let top_cxt = RequestContext::new(TaskKind::GarbageCollector, DownloadBehavior::Download);
        let tenant_cxt = match tenant.get_context(&top_cxt) {
            Ok(cxt) => cxt,
            Err(state) => {
                // This could happen if the tenant is detached or the pageserver is shut
                // down immediately after loading or attaching completed and the tenant
                // was activated. It seems unlikely enough in practice that we better print
                // a warning, as it could also be a bug.
                error!("Not running GC loop, tenant is not active: {state:?}");
                return;
            }
        };
        loop {
            trace!("waking up");

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
                    // Run compaction
                    if let Err(e) = tenant
                        .gc_iteration(None, gc_horizon, tenant.get_pitr_interval(), &tenant_cxt)
                        .await
                    {
                        sleep_duration = wait_duration;
                        error!("Gc failed, retrying in {:?}: {e:?}", sleep_duration);
                    }
                }
            }

            // Sleep
            tokio::select! {
                _ = tenant_cxt.cancelled() => {
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
