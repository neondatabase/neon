//! Contains the route for profiling the compute.
//!
//! Profiling the compute means generating a pprof profile of the
//! postgres processes.
//!
//! The profiling is done using the `perf` tool, which is expected to be
//! available somewhere in `$PATH`.
use std::sync::atomic::Ordering;

use axum::extract::Query;
use axum::response::IntoResponse;
use http::StatusCode;
use nix::unistd::Pid;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::http::JsonResponse;

static CANCEL_CHANNEL: Lazy<Mutex<Option<tokio::sync::oneshot::Sender<()>>>> =
    Lazy::new(|| Mutex::new(None));

fn default_sampling_frequency() -> u16 {
    100
}

fn default_timeout_seconds() -> u8 {
    5
}

fn deserialize_sampling_frequency<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    const MIN_SAMPLING_FREQUENCY: u16 = 1;
    const MAX_SAMPLING_FREQUENCY: u16 = 1000;

    let value = u16::deserialize(deserializer)?;

    if !(MIN_SAMPLING_FREQUENCY..=MAX_SAMPLING_FREQUENCY).contains(&value) {
        return Err(serde::de::Error::custom(format!(
            "sampling_frequency must be between {MIN_SAMPLING_FREQUENCY} and {MAX_SAMPLING_FREQUENCY}, got {value}"
        )));
    }
    Ok(value)
}

fn deserialize_profiling_timeout<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    const MIN_TIMEOUT_SECONDS: u8 = 1;
    const MAX_TIMEOUT_SECONDS: u8 = 60;

    let value = u8::deserialize(deserializer)?;

    if !(MIN_TIMEOUT_SECONDS..=MAX_TIMEOUT_SECONDS).contains(&value) {
        return Err(serde::de::Error::custom(format!(
            "timeout_seconds must be between {MIN_TIMEOUT_SECONDS} and {MAX_TIMEOUT_SECONDS}, got {value}"
        )));
    }
    Ok(value)
}

/// Request parameters for profiling the compute.
#[derive(Debug, Copy, Clone, serde::Deserialize)]
pub(in crate::http) struct ProfileRequest {
    #[serde(default = "default_sampling_frequency")]
    #[serde(deserialize_with = "deserialize_sampling_frequency")]
    sampling_frequency: u16,
    #[serde(default = "default_timeout_seconds")]
    #[serde(deserialize_with = "deserialize_profiling_timeout")]
    timeout_seconds: u8,
}

/// The HTTP request handler for stopping profiling the compute.
pub(in crate::http) async fn profile_stop() -> impl IntoResponse {
    tracing::info!("Profile stop request received.");

    match CANCEL_CHANNEL.lock().await.take() {
        Some(tx) => {
            if tx.send(()).is_err() {
                tracing::error!("Failed to send cancellation signal.");
                return JsonResponse::create_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to send cancellation signal",
                );
            }
            JsonResponse::create_response(StatusCode::OK, "Profiling stopped successfully.")
        }
        None => JsonResponse::create_response(
            StatusCode::PRECONDITION_FAILED,
            "Profiling is not in progress, there is nothing to stop.",
        ),
    }
}

/// The HTTP request handler for starting profiling the compute.
pub(in crate::http) async fn profile_start(
    Query(request): Query<ProfileRequest>,
) -> impl IntoResponse {
    tracing::info!("Profile start request received: {request:?}");

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    match CANCEL_CHANNEL.try_lock() {
        Ok(mut cancel_channel) => {
            if cancel_channel.is_some() {
                return JsonResponse::create_response(
                    StatusCode::CONFLICT,
                    "Profiling is already in progress.",
                );
            } else {
                *cancel_channel = Some(tx);
            }
        }
        Err(_) => {
            return JsonResponse::create_response(
                StatusCode::ALREADY_REPORTED,
                "Another request is being processed.",
            );
        }
    }

    tracing::info!("Profiling will start with parameters: {request:?}");
    let pg_pid = Pid::from_raw(crate::compute::PG_PID.load(Ordering::SeqCst) as _);

    let options = crate::profiling::ProfileGenerationOptions {
        #[cfg(feature = "testing")]
        run_with_sudo: false,
        #[cfg(not(feature = "testing"))]
        run_with_sudo: true,
        perf_binary_path: None,
        process_pid: pg_pid,
        follow_forks: true,
        sampling_frequency: request.sampling_frequency as u32,
        blocklist_symbols: &["libc", "libgcc", "pthread", "vdso"],
        timeout: std::time::Duration::from_secs(request.timeout_seconds as u64),
        should_stop: Some(rx),
    };

    let pprof_data = crate::profiling::generate_pprof_using_perf(options).await;

    if CANCEL_CHANNEL.lock().await.take().is_none() {
        tracing::error!("Profiling was cancelled from another request.");

        return JsonResponse::create_response(
            StatusCode::NO_CONTENT,
            "Profiling was cancelled from another request.",
        );
    }

    let pprof_data = match pprof_data {
        Ok(data) => data,
        Err(e) => {
            // tracing::error!(%e, "failed to generate pprof data");
            return JsonResponse::create_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to generate pprof data: {e}"),
            );
        }
    };

    tracing::info!("Profiling has completed successfully.");

    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        http::header::CONTENT_DISPOSITION,
        http::HeaderValue::from_static("attachment; filename=\"profile.pb\""),
    );

    (headers, pprof_data.0).into_response()
}
