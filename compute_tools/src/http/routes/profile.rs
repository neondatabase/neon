//! Contains the route for profiling the compute.
//!
//! Profiling the compute means generating a pprof profile of the
//! postgres processes.
//!
//! The profiling is done using the `perf` tool, which is expected to be
//! available somewhere in `$PATH`.
use std::sync::atomic::Ordering;

use axum::Json;
use axum::response::IntoResponse;
use http::StatusCode;
use nix::unistd::Pid;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::http::JsonResponse;

static CANCEL_CHANNEL: Lazy<Mutex<Option<tokio::sync::broadcast::Sender<()>>>> =
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
#[derive(Debug, Clone, serde::Deserialize)]
pub(in crate::http) struct ProfileRequest {
    /// The profiling tool to use, currently only `perf` is supported.
    profiler: crate::profiling::ProfileGenerator,
    #[serde(default = "default_sampling_frequency")]
    #[serde(deserialize_with = "deserialize_sampling_frequency")]
    sampling_frequency: u16,
    #[serde(default = "default_timeout_seconds")]
    #[serde(deserialize_with = "deserialize_profiling_timeout")]
    timeout_seconds: u8,
    #[serde(default)]
    archive: bool,
}

/// The HTTP request handler for reporting the profiling status of
/// the compute.
pub(in crate::http) async fn profile_status() -> impl IntoResponse {
    tracing::info!("Profile status request received.");

    let cancel_channel = CANCEL_CHANNEL.lock().await;

    if let Some(tx) = cancel_channel.as_ref() {
        if tx.receiver_count() > 0 {
            return JsonResponse::create_response(
                StatusCode::OK,
                "Profiling is currently in progress.",
            );
        }
    }

    JsonResponse::create_response(StatusCode::NO_CONTENT, "Profiling is not in progress.")
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
    Json(request): Json<ProfileRequest>,
) -> impl IntoResponse {
    tracing::info!("Profile start request received: {request:?}");

    let tx = tokio::sync::broadcast::Sender::<()>::new(1);

    {
        let mut cancel_channel = CANCEL_CHANNEL.lock().await;

        if cancel_channel.is_some() {
            return JsonResponse::create_response(
                StatusCode::CONFLICT,
                "Profiling is already in progress.",
            );
        }
        *cancel_channel = Some(tx.clone());
    }

    tracing::info!("Profiling will start with parameters: {request:?}");
    let pg_pid = Pid::from_raw(crate::compute::PG_PID.load(Ordering::SeqCst) as _);

    let run_with_sudo = !cfg!(feature = "testing");

    let options = crate::profiling::ProfileGenerationOptions {
        profiler: request.profiler,
        run_with_sudo,
        pids: [pg_pid].into_iter().collect(),
        follow_forks: true,
        sampling_frequency: request.sampling_frequency as u32,
        blocklist_symbols: vec![
            "libc".to_owned(),
            "libgcc".to_owned(),
            "pthread".to_owned(),
            "vdso".to_owned(),
        ],
        archive: request.archive,
    };

    let options = crate::profiling::ProfileGenerationTaskOptions {
        options,
        timeout: std::time::Duration::from_secs(request.timeout_seconds as u64),
        should_stop: Some(tx),
    };

    let pprof_data = crate::profiling::generate_pprof_profile(options).await;

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
            tracing::error!(error = ?e, "failed to generate pprof data");
            return JsonResponse::create_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to generate pprof data: {e:?}"),
            );
        }
    };

    tracing::info!("Profiling has completed successfully.");

    let mut headers = http::HeaderMap::new();

    if request.archive {
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/gzip"),
        );
        headers.insert(
            http::header::CONTENT_DISPOSITION,
            http::HeaderValue::from_static("attachment; filename=\"profile.pb.gz\""),
        );
    } else {
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(
            http::header::CONTENT_DISPOSITION,
            http::HeaderValue::from_static("attachment; filename=\"profile.pb\""),
        );
    }

    (headers, pprof_data.0).into_response()
}
