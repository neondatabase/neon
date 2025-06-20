//! Contains the route for profiling the compute.
//!
//! Profiling the compute means generating a pprof profile of the
//! postgres processes.
//!
//! The profiling is done using the `perf` tool, which is expected to be
//! available at `/usr/bin/perf`.
use std::io::Read;
use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use nix::unistd::Pid;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

const PERF_BINARY_PATH: &str = "/usr/bin/perf";

fn default_sampling_frequency() -> u16 {
    100
}

fn default_timeout_seconds() -> u8 {
    5
}

/// Request parameters for profiling the compute.
#[derive(Debug, Copy, Clone, serde::Deserialize)]
pub(in crate::http) struct ProfileRequest {
    #[serde(default = "default_sampling_frequency")]
    sampling_frequency: u16,
    #[serde(default = "default_timeout_seconds")]
    timeout_seconds: u8,
    #[serde(default)]
    should_stop: bool,
}

impl ProfileRequest {
    fn validate(&self) -> Result<(), String> {
        if self.sampling_frequency < 1 || self.sampling_frequency > 1000 {
            return Err("sampling_frequency must be between 1 and 1000".to_string());
        }
        if self.timeout_seconds < 1 || self.timeout_seconds > 60 {
            return Err("timeout_seconds must be between 1 and 60".to_string());
        }
        Ok(())
    }
}

fn create_response<B: Into<Body>>(status: StatusCode, body: B) -> Response {
    let mut r = status.into_response();
    *r.body_mut() = body.into();
    r
}

/// The HTTP request handler for profiling the compute.
pub(in crate::http) async fn profile(Query(request): Query<ProfileRequest>) -> Response {
    static CANCEL_CHANNEL: Lazy<Mutex<Option<crossbeam_channel::Sender<()>>>> =
        Lazy::new(|| Mutex::new(None));

    tracing::info!("Profile request received: {request:?}");

    if let Err(e) = request.validate() {
        return create_response(
            StatusCode::BAD_REQUEST,
            format!("Invalid request parameters: {e}"),
        );
    }

    let (tx, rx) = crossbeam_channel::bounded::<()>(1);

    match CANCEL_CHANNEL.try_lock() {
        Ok(mut cancel_channel) => {
            if request.should_stop {
                if let Some(old_tx) = cancel_channel.take() {
                    if let Err(e) = old_tx.send(()) {
                        tracing::error!("Failed to send cancellation signal: {e}");
                        return create_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to send cancellation signal",
                        );
                    } else {
                        return create_response(StatusCode::OK, "Profiling stopped successfully.");
                    }
                } else {
                    return create_response(
                        StatusCode::NO_CONTENT,
                        "Profiling is not in progress, there is nothing to stop.",
                    );
                }
            } else if cancel_channel.is_some() {
                return create_response(StatusCode::CONFLICT, "Profiling is already in progress.");
            } else {
                *cancel_channel = Some(tx);
            }
        }
        Err(_) => {
            return create_response(
                StatusCode::ALREADY_REPORTED,
                "Another request is being processed.",
            );
        }
    }

    tracing::info!("Profiling will start with parameters: {request:?}");
    let pg_pid = Pid::from_raw(crate::compute::PG_PID.load(Ordering::SeqCst) as _);

    let options = crate::profiling::ProfileGenerationOptions {
        run_with_sudo: true,
        perf_binary_path: Some(PERF_BINARY_PATH.as_ref()),
        process_pid: pg_pid,
        follow_forks: true,
        sampling_frequency: Some(request.sampling_frequency as u32),
        blocklist_symbols: &["libc", "libgcc", "pthread", "vdso"],
        timeout: std::time::Duration::from_secs(request.timeout_seconds as u64),
        should_stop: Some(rx),
    };
    let pprof_data = match crate::profiling::generate_pprof_using_perf(options) {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to generate pprof data: {e}");
            return create_response(
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
