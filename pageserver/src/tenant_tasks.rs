//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::ops::ControlFlow;
use std::time::Duration;

use crate::layered_repository::TenantState;
use crate::repository::Repository;
use crate::RepositoryImpl;
use anyhow;
use std::sync::Arc;
use tracing::*;

pub fn start_background_loops(repo: &Arc<RepositoryImpl>) {
    let repo_clone = Arc::clone(repo);
    tokio::spawn(async { crate::tenant_tasks::compaction_loop(repo_clone) });
    let repo_clone = Arc::clone(repo);
    tokio::spawn(async { crate::tenant_tasks::gc_loop(repo_clone) });
}

///
/// Compaction task's main loop
///
pub async fn compaction_loop(repo: Arc<RepositoryImpl>) {
    loop {
        trace!("waking up");

        // Run blocking part of the task
        let repo = Arc::clone(&repo);
        let period: Result<Result<_, anyhow::Error>, _> = tokio::task::spawn_blocking(move || {
            // Break if tenant is not active
            if repo.get_state() != TenantState::Active {
                return Ok(ControlFlow::Break(()));
            }

            // Break if we're not allowed to write to disk
            // TODO do this inside repo.compaction_iteration instead.
            let _guard = match repo.file_lock.try_read() {
                Ok(g) => g,
                Err(_) => return Ok(ControlFlow::Break(())),
            };

            // Run compaction
            let compaction_period = repo.get_compaction_period();
            repo.compaction_iteration()?;
            Ok(ControlFlow::Continue(compaction_period))
        })
        .await;

        // Decide whether to sleep or break
        let sleep_duration = match period {
            Ok(Ok(ControlFlow::Continue(period))) => period,
            Ok(Ok(ControlFlow::Break(()))) => break,
            Ok(Err(e)) => {
                error!("Compaction failed, retrying: {}", e);
                Duration::from_secs(2)
            }
            Err(e) => {
                error!("Compaction join error, retrying: {}", e);
                Duration::from_secs(2)
            }
        };

        // Sleep
        // FIXME: cancellation
        tokio::select! {
                /*
                    _ = cancel.changed() => {
                        trace!("received cancellation request");
                        break;
                    },
        */
                    _ = tokio::time::sleep(sleep_duration) => {},
                }
    }

    trace!("compaction loop stopped. State is {:?}", repo.get_state());
}

///
/// GC task's main loop
///
pub async fn gc_loop(repo: Arc<RepositoryImpl>) {
    loop {
        trace!("waking up");

        // Run blocking part of the task
        let repo = Arc::clone(&repo);
        let period: Result<Result<_, anyhow::Error>, _> = tokio::task::spawn_blocking(move || {
            // Break if tenant is not active
            if repo.get_state() != TenantState::Active {
                return Ok(ControlFlow::Break(()));
            }

            // Break if we're not allowed to write to disk
            // TODO do this inside repo.gc_iteration instead.
            let _guard = match repo.file_lock.try_read() {
                Ok(g) => g,
                Err(_) => return Ok(ControlFlow::Break(())),
            };

            // Run gc
            let gc_period = repo.get_gc_period();
            let gc_horizon = repo.get_gc_horizon();
            if gc_horizon > 0 {
                repo.gc_iteration(None, gc_horizon, repo.get_pitr_interval(), false)?;
            }

            Ok(ControlFlow::Continue(gc_period))
        })
        .await;

        // Decide whether to sleep or break
        let sleep_duration = match period {
            Ok(Ok(ControlFlow::Continue(period))) => period,
            Ok(Ok(ControlFlow::Break(()))) => break,
            Ok(Err(e)) => {
                error!("Gc failed, retrying: {}", e);
                Duration::from_secs(2)
            }
            Err(e) => {
                error!("Gc join error, retrying: {}", e);
                Duration::from_secs(2)
            }
        };

        // Sleep
        tokio::select! {
            /*
            _ = cancel.changed() => {
                trace!("received cancellation request");
                break;
            },
             */
            _ = tokio::time::sleep(sleep_duration) => {},
        }
    }
    trace!("GC loop stopped. State is {:?}", repo.get_state());
}
