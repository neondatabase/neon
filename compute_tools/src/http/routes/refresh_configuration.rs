// This file is added by Hadron

use std::sync::Arc;

use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use http::StatusCode;

use crate::compute::ComputeNode;
use crate::hadron_metrics::POSTGRES_PAGESTREAM_REQUEST_ERRORS;
use crate::http::JsonResponse;

/// The /refresh_configuration POST method is used to nudge compute_ctl to pull a new spec
/// from the HCC and attempt to reconfigure Postgres with the new spec. The method does not wait
/// for the reconfiguration to complete. Rather, it simply delivers a signal that will cause
/// configuration to be reloaded in a best effort manner. Invocation of this method does not
/// guarantee that a reconfiguration will occur. The caller should consider keep sending this
/// request while it believes that the compute configuration is out of date.
pub(in crate::http) async fn refresh_configuration(
    State(compute): State<Arc<ComputeNode>>,
) -> Response {
    POSTGRES_PAGESTREAM_REQUEST_ERRORS.inc();
    match compute.signal_refresh_configuration().await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
