use std::sync::Arc;

use crate::compute::ComputeNode;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{ErrorResponse, IntoResponse, Response, Result};
type State = AxumState<std::sync::Arc<ComputeNode>>;

fn to_axum_err(err: impl std::fmt::Display) -> ErrorResponse {
    tracing::error!(%err, "LFC prewarm error");
    StatusCode::INTERNAL_SERVER_ERROR.into()
}

fn get_addr_token(state: &ComputeNode) -> (String, String) {
    let addr: String;
    let token: String;
    {
        let state = state.state.lock().unwrap();
        let pspec = state.pspec.as_ref().expect("pspec must be provided");
        addr = pspec.object_storage_addr.clone();
        token = pspec.object_storage_token.clone();
    }
    (addr, token)
}

pub(in crate::http) async fn prewarm_lfc(AxumState(state): State) -> Result<()> {
    crate::metrics::LFC_PREWARM_REQUESTS.inc();
    let (addr, token) = get_addr_token(&state);
    Ok(())
}

pub(in crate::http) async fn prewarm_lfc_offload(AxumState(state): State) -> Result<()> {
    crate::metrics::LFC_PREWARM_OFFLOAD_REQUESTS.inc();

    let (addr, token) = get_addr_token(&state);
    let lfc_state = ComputeNode::get_maintenance_client(&state.tokio_conn_conf)
        .await
        .map_err(to_axum_err)?
        .query_one("select get_local_cache_state()", &[])
        .await
        .map_err(to_axum_err)?
        .try_get::<usize, String>(0)
        .map_err(to_axum_err)?;

    // connect to service, upload file
    Ok(())
}
