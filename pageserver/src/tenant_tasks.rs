//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::time::Duration;

use crate::layered_repository::{Repository, TenantState};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use std::sync::Arc;
use tracing::*;

pub fn start_background_loops(repo: &Arc<Repository>) {
    let tenant_id = repo.tenant_id();
    let repo_clone = Arc::clone(repo);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        Some(tenant_id),
        None,
        &format!("compactor for tenant {tenant_id}"),
        false,
        async {
            compaction_loop(repo_clone).await;
            Ok(())
        },
    );
    let repo_clone = Arc::clone(repo);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        None,
        &format!("garbage collector for tenant {tenant_id}"),
        false,
        async {
            gc_loop(repo_clone).await;
            Ok(())
        },
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(repo: Arc<Repository>) {
    loop {
        trace!("waking up");

        // Run blocking part of the task
        let repo = Arc::clone(&repo);
        let sleep_duration = {
            // Break if tenant is not active
            if repo.get_state() != TenantState::Active {
                break;
            }

            // Run compaction
            let compaction_period = repo.get_compaction_period();
            if let Err(e) = repo.compaction_iteration().await {
                error!("Compaction failed, retrying: {}", e);
                Duration::from_secs(2)
            } else {
                compaction_period
            }
        };

        // Sleep
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                trace!("received cancellation request");
                break;
            },
            _ = tokio::time::sleep(sleep_duration) => {},
        }
    }

    trace!("compaction loop stopped. State is {:?}", repo.get_state());
}

///
/// GC task's main loop
///
async fn gc_loop(repo: Arc<Repository>) {
    loop {
        trace!("waking up");

        let gc_period = repo.get_gc_period();
        let gc_horizon = repo.get_gc_horizon();

        // Run blocking part of the task
        let repo = Arc::clone(&repo);
        let mut sleep_duration = gc_period;

        // Break if tenant is not active
        if repo.get_state() != TenantState::Active {
            break;
        }

        // Run gc
        if gc_horizon > 0 {
            if let Err(e) = repo
                .gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)
                .await
            {
                error!("Gc failed, retrying: {}", e);
                sleep_duration = Duration::from_secs(2)
            }
        }

        // Sleep
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                trace!("received cancellation request");
                break;
            },
            _ = tokio::time::sleep(sleep_duration) => {},
        }
    }
    trace!("GC loop stopped. State is {:?}", repo.get_state());
}
