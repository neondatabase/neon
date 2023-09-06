//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::context::{DownloadBehavior, RequestContext};
use crate::metrics::TENANT_TASK_EVENTS;
use crate::task_mgr;
use crate::task_mgr::{TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::{Tenant, TenantState};
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::completion;

/// Start per tenant background loops: compaction and gc.
pub fn start_background_loops(
    tenant: &Arc<Tenant>,
    background_jobs_can_start: Option<&completion::Barrier>,
) {
    let tenant_id = tenant.tenant_id;
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        Some(tenant_id),
        None,
        &format!("compactor for tenant {tenant_id}"),
        false,
        {
            let tenant = Arc::clone(tenant);
            let background_jobs_can_start = background_jobs_can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token();
                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()) },
                    _ = completion::Barrier::maybe_wait(background_jobs_can_start) => {}
                };
                compaction_loop(tenant, cancel)
                    .instrument(info_span!("compaction_loop", tenant_id = %tenant_id))
                    .await;
                Ok(())
            }
        },
    );
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        None,
        &format!("garbage collector for tenant {tenant_id}"),
        false,
        {
            let tenant = Arc::clone(tenant);
            let background_jobs_can_start = background_jobs_can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token();
                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()) },
                    _ = completion::Barrier::maybe_wait(background_jobs_can_start) => {}
                };
                gc_loop(tenant, cancel)
                    .instrument(info_span!("gc_loop", tenant_id = %tenant_id))
                    .await;
                Ok(())
            }
        },
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(tenant: Arc<Tenant>, cancel: CancellationToken) {
    let wait_duration = Duration::from_secs(2);
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        let ctx = RequestContext::todo_child(TaskKind::Compaction, DownloadBehavior::Download);
        let mut first = true;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return;
                },
                tenant_wait_result = wait_for_active_tenant(&tenant) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(()) => (),
                },
            }

            let period = tenant.get_compaction_period();

            // TODO: we shouldn't need to await to find tenant and this could be moved outside of
            // loop, #3501. There are also additional "allowed_errors" in tests.
            if first {
                first = false;
                if random_init_delay(period, &cancel).await.is_err() {
                    break;
                }
            }

            let started_at = Instant::now();

            let sleep_duration = if period == Duration::ZERO {
                #[cfg(not(feature = "testing"))]
                info!("automatic compaction is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                Duration::from_secs(10)
            } else {
                // Run compaction
                if let Err(e) = tenant.compaction_iteration(&cancel, &ctx).await {
                    error!("Compaction failed, retrying in {:?}: {e:?}", wait_duration);
                    wait_duration
                } else {
                    period
                }
            };

            warn_when_period_overrun(started_at.elapsed(), period, "compaction");

            // Sleep
            if tokio::time::timeout(sleep_duration, cancel.cancelled())
                .await
                .is_ok()
            {
                break;
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
}

///
/// GC task's main loop
///
async fn gc_loop(tenant: Arc<Tenant>, cancel: CancellationToken) {
    let wait_duration = Duration::from_secs(2);
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        // GC might require downloading, to find the cutoff LSN that corresponds to the
        // cutoff specified as time.
        let ctx =
            RequestContext::todo_child(TaskKind::GarbageCollector, DownloadBehavior::Download);
        let mut first = true;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return;
                },
                tenant_wait_result = wait_for_active_tenant(&tenant) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(()) => (),
                },
            }

            let period = tenant.get_gc_period();

            if first {
                first = false;
                if random_init_delay(period, &cancel).await.is_err() {
                    break;
                }
            }

            let started_at = Instant::now();

            let gc_horizon = tenant.get_gc_horizon();
            let sleep_duration = if period == Duration::ZERO || gc_horizon == 0 {
                #[cfg(not(feature = "testing"))]
                info!("automatic GC is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                Duration::from_secs(10)
            } else {
                // Run gc
                let res = tenant
                    .gc_iteration(None, gc_horizon, tenant.get_pitr_interval(), &ctx)
                    .await;
                if let Err(e) = res {
                    error!("Gc failed, retrying in {:?}: {e:?}", wait_duration);
                    wait_duration
                } else {
                    period
                }
            };

            warn_when_period_overrun(started_at.elapsed(), period, "gc");

            // Sleep
            if tokio::time::timeout(sleep_duration, cancel.cancelled())
                .await
                .is_ok()
            {
                break;
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
}

async fn wait_for_active_tenant(tenant: &Arc<Tenant>) -> ControlFlow<()> {
    // if the tenant has a proper status already, no need to wait for anything
    if tenant.current_state() == TenantState::Active {
        ControlFlow::Continue(())
    } else {
        let mut tenant_state_updates = tenant.subscribe_for_state_updates();
        loop {
            match tenant_state_updates.changed().await {
                Ok(()) => {
                    let new_state = &*tenant_state_updates.borrow();
                    match new_state {
                        TenantState::Active => {
                            debug!("Tenant state changed to active, continuing the task loop");
                            return ControlFlow::Continue(());
                        }
                        state => {
                            debug!("Not running the task loop, tenant is not active: {state:?}");
                            continue;
                        }
                    }
                }
                Err(_sender_dropped_error) => {
                    return ControlFlow::Break(());
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("cancelled")]
pub(crate) struct Cancelled;

/// Provide a random delay for background task initialization.
///
/// This delay prevents a thundering herd of background tasks and will likely keep them running on
/// different periods for more stable load.
pub(crate) async fn random_init_delay(
    period: Duration,
    cancel: &CancellationToken,
) -> Result<(), Cancelled> {
    use rand::Rng;

    if period == Duration::ZERO {
        return Ok(());
    }

    let d = {
        let mut rng = rand::thread_rng();
        rng.gen_range(Duration::ZERO..=period)
    };

    match tokio::time::timeout(d, cancel.cancelled()).await {
        Ok(_) => Err(Cancelled),
        Err(_) => Ok(()),
    }
}

/// Attention: the `task` and `period` beocme labels of a pageserver-wide prometheus metric.
pub(crate) fn warn_when_period_overrun(elapsed: Duration, period: Duration, task: &str) {
    // Duration::ZERO will happen because it's the "disable [bgtask]" value.
    if elapsed >= period && period != Duration::ZERO {
        // humantime does no significant digits clamping whereas Duration's debug is a bit more
        // intelligent. however it makes sense to keep the "configuration format" for period, even
        // though there's no way to output the actual config value.
        warn!(
            ?elapsed,
            period = %humantime::format_duration(period),
            task,
            "task iteration took longer than the configured period"
        );
        crate::metrics::BACKGROUND_LOOP_PERIOD_OVERRUN_COUNT
            .with_label_values(&[task, &format!("{}", period.as_secs())])
            .inc();
    }
}
