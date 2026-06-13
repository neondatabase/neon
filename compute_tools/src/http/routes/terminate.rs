use crate::compute::{ComputeNode, forward_termination_signal};
use crate::http::JsonResponse;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum_extra::extract::OptionalQuery;
use compute_api::responses::{
    ComputeStatus, TerminateMode, TerminateNotIdleResponse, TerminateResponse,
};
use http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tracing::{info, warn};

#[derive(Deserialize, Default)]
pub struct TerminateQuery {
    #[serde(default)]
    mode: TerminateMode,
    /// If set, only terminate when the compute is idle (no client sessions,
    /// logical walsenders, or autovacuum workers). If it is not idle, the
    /// compute is left running and `/terminate` returns 409 Conflict.
    #[serde(default)]
    if_idle: bool,
}

/// Check whether the compute is idle by querying Postgres directly, returning
/// the current counts (the caller decides idleness = all zero). Mirrors the
/// queries the activity monitor uses; `num_client_sessions` excludes internal
/// `cloud_admin` connections (e.g. compute_ctl's own monitor).
async fn check_idle(compute: &ComputeNode) -> anyhow::Result<TerminateNotIdleResponse> {
    // Same predicate as the activity monitor's get_backends_state_change():
    // exclude this very connection (pg_backend_pid) and internal cloud_admin
    // connections (compute_ctl's own monitor, vm-monitor, exporters, ...).
    const CLIENT_SESSIONS_QUERY: &str = "select count(*) from pg_stat_activity \
         where backend_type = 'client backend' \
         and pid != pg_backend_pid() and usename != 'cloud_admin';";
    const WALSENDERS_QUERY: &str =
        "select count(*) from pg_stat_replication where application_name != 'walproposer';";
    const AUTOVACUUM_QUERY: &str =
        "select count(*) from pg_stat_activity where backend_type = 'autovacuum worker';";

    let conf = compute.get_tokio_conn_conf(Some("compute_ctl:terminate_idle_check"));
    let (client, connection) = conf.connect(tokio_postgres::NoTls).await?;
    let conn_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!("terminate idle-check connection error: {}", e);
        }
    });

    let num_client_sessions: i64 = client
        .query_one(CLIENT_SESSIONS_QUERY, &[])
        .await?
        .try_get("count")?;
    let num_walsenders: i64 = client
        .query_one(WALSENDERS_QUERY, &[])
        .await?
        .try_get("count")?;
    let num_autovacuum_workers: i64 = client
        .query_one(AUTOVACUUM_QUERY, &[])
        .await?
        .try_get("count")?;

    conn_task.abort();
    Ok(TerminateNotIdleResponse {
        num_client_sessions,
        num_walsenders,
        num_autovacuum_workers,
    })
}

/// Terminate the compute.
pub(in crate::http) async fn terminate(
    State(compute): State<Arc<ComputeNode>>,
    OptionalQuery(terminate): OptionalQuery<TerminateQuery>,
) -> Response {
    let query = terminate.unwrap_or_default();
    let mode = query.mode;

    // When `if_idle` is set, only terminate a Running compute if it has no
    // client sessions, logical walsenders, or autovacuum workers. This collapses
    // the "probe for idleness, then terminate" sequence an external control plane
    // would otherwise do over two HTTP calls into a single call. NOTE: it narrows
    // but does not fully eliminate the race -- a connection that arrives after the
    // check but before Postgres shuts down still loses -- so an external barrier
    // (e.g. restricting who may connect) remains necessary for a hard guarantee.
    // Non-Running statuses (e.g. Empty) have no Postgres / no client sessions, so
    // they fall through to normal termination.
    if query.if_idle {
        let status = compute.state.lock().unwrap().status;
        if status == ComputeStatus::Running {
            // Bound the check: Postgres may be up but unresponsive (saturated,
            // blocked, slow pageserver). Without a timeout the await could hang
            // indefinitely; on timeout we take the same fail-safe path as an
            // outright error and do NOT terminate.
            const IDLE_CHECK_TIMEOUT: Duration = Duration::from_secs(10);
            match tokio::time::timeout(IDLE_CHECK_TIMEOUT, check_idle(&compute)).await {
                Ok(Ok(counts)) => {
                    if counts.num_client_sessions > 0
                        || counts.num_walsenders > 0
                        || counts.num_autovacuum_workers > 0
                    {
                        info!(
                            "not terminating: compute is not idle ({} client sessions, {} walsenders, {} autovacuum workers)",
                            counts.num_client_sessions,
                            counts.num_walsenders,
                            counts.num_autovacuum_workers
                        );
                        return JsonResponse::create_response(StatusCode::CONFLICT, counts);
                    }
                }
                Ok(Err(e)) => {
                    // Fail safe: if we cannot determine idleness, do NOT terminate.
                    warn!("if_idle check failed, not terminating: {}", e);
                    return JsonResponse::error(StatusCode::SERVICE_UNAVAILABLE, e);
                }
                Err(_elapsed) => {
                    warn!("if_idle check timed out, not terminating");
                    return JsonResponse::error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "idle check timed out",
                    );
                }
            }
        }
    }

    {
        let mut state = compute.state.lock().unwrap();
        if state.status == ComputeStatus::Terminated {
            let response = TerminateResponse {
                lsn: state.terminate_flush_lsn,
            };
            return JsonResponse::success(StatusCode::CREATED, response);
        }

        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        // If compute is Empty, there's no Postgres to terminate. The regular compute_ctl termination path
        // assumes Postgres to be configured and running, so we just special-handle this case by exiting
        // the process directly.
        if compute.params.lakebase_mode && state.status == ComputeStatus::Empty {
            drop(state);
            info!("terminating empty compute - will exit process");

            // Queue a task to exit the process after 5 seconds. The 5-second delay aims to
            // give enough time for the HTTP response to be sent so that HCM doesn't get an abrupt
            // connection termination.
            tokio::spawn(async {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                info!("exiting process after terminating empty compute");
                std::process::exit(0);
            });

            return StatusCode::OK.into_response();
        }

        // For Running status, proceed with normal termination
        state.set_status(mode.into(), &compute.state_changed);
        drop(state);
    }

    forward_termination_signal(false);
    info!("sent signal and notified waiters");

    // Spawn a blocking thread to wait for compute to become Terminated.
    // This is needed to do not block the main pool of workers and
    // be able to serve other requests while some particular request
    // is waiting for compute to finish configuration.
    let c = compute.clone();
    let lsn = task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        while state.status != ComputeStatus::Terminated {
            state = c.state_changed.wait(state).unwrap();
            info!(
                "waiting for compute to become {}, current status: {:?}",
                ComputeStatus::Terminated,
                state.status
            );
        }
        state.terminate_flush_lsn
    })
    .await
    .unwrap();
    info!("terminated Postgres");
    JsonResponse::success(StatusCode::OK, TerminateResponse { lsn })
}
