use crate::compute::ComputeNode;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{ErrorResponse, Result};
use reqwest::Client;
type State = AxumState<std::sync::Arc<ComputeNode>>;

fn to_axum_err(err: impl std::fmt::Display) -> ErrorResponse {
    tracing::error!(%err, "LFC prewarm error");
    StatusCode::INTERNAL_SERVER_ERROR.into()
}

fn get_addr_token(state: &ComputeNode) -> (String, String) {
    let state = state.state.lock().unwrap();
    let pspec = state.pspec.as_ref().expect("pspec must be provided");
    (
        pspec.endpoint_storage_addr.clone(),
        pspec.endpoint_storage_token.clone(),
    )
}

const KEY: &str = "lfc_state";

pub(in crate::http) async fn prewarm_lfc(AxumState(state): State) -> Result<()> {
    crate::metrics::LFC_PREWARM_REQUESTS.inc();
    let (_addr, _token) = get_addr_token(&state);
    Ok(())
}

// TODO JsonResponse
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

    let client = Client::new();
    let res = client
        .put(format!("{addr}/{KEY}"))
        .bearer_auth(token)
        .body(lfc_state)
        .send()
        .await;
    match res {
        Ok(response) if response.status() == StatusCode::OK => return Ok(()),
        Ok(_) => {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        }
        Err(_) => {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        }
    }
}
