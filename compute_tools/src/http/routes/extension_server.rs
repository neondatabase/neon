use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use serde::Deserialize;

use crate::compute::{BUILD_TAG, ComputeNode};
use crate::http::JsonResponse;
use crate::http::extract::{Path, Query};

#[derive(Debug, Clone, Deserialize)]
pub(in crate::http) struct ExtensionServerParams {
    #[serde(default)]
    is_library: bool,
}

/// Download a remote extension.
pub(in crate::http) async fn download_extension(
    Path(filename): Path<String>,
    ext_server_params: Query<ExtensionServerParams>,
    State(compute): State<Arc<ComputeNode>>,
) -> Response {
    // Don't even try to download extensions if no remote storage is configured
    if compute.params.remote_ext_base_url.is_none() {
        return JsonResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "remote storage is not configured",
        );
    }

    let ext = {
        let state = compute.state.lock().unwrap();
        let pspec = state.pspec.as_ref().unwrap();
        let spec = &pspec.spec;

        let remote_extensions = match spec.remote_extensions.as_ref() {
            Some(r) => r,
            None => {
                return JsonResponse::error(
                    StatusCode::CONFLICT,
                    "information about remote extensions is unavailable",
                );
            }
        };

        remote_extensions.get_ext(
            &filename,
            ext_server_params.is_library,
            &BUILD_TAG,
            &compute.params.pgversion,
        )
    };

    match ext {
        Ok((ext_name, ext_path)) => match compute.download_extension(ext_name, ext_path).await {
            Ok(_) => StatusCode::OK.into_response(),
            Err(e) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e),
        },
        Err(e) => JsonResponse::error(StatusCode::NOT_FOUND, e),
    }
}
