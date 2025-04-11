use crate::compute::ComputeNode;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{ErrorResponse, Result};
use reqwest::Client;
use tracing::info;
use utils::id::{TenantId, TimelineId};
type State = AxumState<std::sync::Arc<ComputeNode>>;

fn to_axum_err(err: impl std::fmt::Display) -> ErrorResponse {
    tracing::error!(%err, "LFC prewarm error");
    StatusCode::INTERNAL_SERVER_ERROR.into()
}

fn get_addr_token(state: &ComputeNode) -> (String, String) {
    let state = state.state.lock().unwrap();
    let pspec = state.pspec.as_ref().expect("pspec must be provided");
    let addr = pspec.endpoint_storage_addr.clone();
    let token = pspec.endpoint_storage_token.clone();
    tracing::info!(%addr, %token, "LFC prewarm");
    (addr, token)
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

    info!("Requesting LFC state from Postgres");

    let (addr, token) = get_addr_token(&state);
    let lfc_state = ComputeNode::get_maintenance_client(&state.tokio_conn_conf)
        .await
        .map_err(to_axum_err)?
        .query_one("select get_local_cache_state()", &[])
        .await
        .map_err(to_axum_err)?
        .try_get::<usize, &[u8]>(0)
        .map_err(to_axum_err)?
        .to_vec();

    info!("Downloaded lfc state of size {}", lfc_state.len());

    let tenant_id: TenantId;
    let timeline_id: TimelineId;
    let endpoint_id: String;
    {
        let state = state.state.lock().unwrap();
        let pspec = state.pspec.as_ref().expect("pspec must be provided");
        tenant_id = pspec.tenant_id;
        timeline_id = pspec.timeline_id;
        timeline_id = pspec.spec.endpoint_id;
    }

    let uri = format!("{addr}/{tenant_id}/{timeline_id}/{endpoint_id}/{KEY}");
    info!(%uri, "Sending LFC state to endpoint storage");

    let res = Client::new()
        .put(uri)
        .bearer_auth(token)
        .body(lfc_state)
        .send()
        .await;
    match res {
        Ok(res) if res.status() == StatusCode::OK => Ok(()),
        Ok(res) => Err(to_axum_err(res.status())),
        Err(err) => Err(to_axum_err(err)),
    }
}
