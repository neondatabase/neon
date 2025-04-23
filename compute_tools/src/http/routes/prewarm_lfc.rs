use crate::compute::ComputeNode;
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use axum::Json;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use compute_api::responses::PrewarmStatus::{self, *};
use reqwest::Client;
use serde::Serialize;
use std::sync::Arc;
use tokio::{io::AsyncReadExt, spawn};
use tracing::{error, info};

type State = AxumState<Arc<ComputeNode>>;
type UnitResult = Result<(), ()>;
const KEY: &str = "lfc_state";

pub struct Parts {
    uri: String,
    token: String,
}

impl TryFrom<&Arc<ComputeNode>> for Parts {
    type Error = &'static str;
    fn try_from(state: &Arc<ComputeNode>) -> Result<Self, Self::Error> {
        let state = state.state.lock().unwrap();
        let Some(pspec) = state.pspec.as_ref() else {
            return Err("pspec is not present");
        };

        let endpoint_id = pspec.spec.endpoint_id.as_ref();
        let Some(endpoint_id) = endpoint_id else {
            return Err("pspec.endpoint_id missing");
        };
        let endpoint_id = endpoint_id.clone();

        let base_uri = &pspec.endpoint_storage_addr;
        let tenant_id = pspec.tenant_id;
        let timeline_id = pspec.timeline_id;

        let uri = format!("{base_uri}/{tenant_id}/{timeline_id}/{endpoint_id}/{KEY}",);
        let token = pspec.endpoint_storage_token.clone();
        Ok(Parts { uri, token })
    }
}

impl axum::extract::FromRequestParts<Arc<ComputeNode>> for Parts {
    type Rejection = Response;
    async fn from_request_parts(
        _: &mut http::request::Parts,
        state: &Arc<ComputeNode>,
    ) -> Result<Self, Self::Rejection> {
        state.try_into().map_err(|e| {
            error!("{e}");
            StatusCode::BAD_REQUEST.into_response()
        })
    }
}

pub(in crate::http) async fn prewarm_lfc_offload(parts: Parts, state: State) {
    spawn(async move { prewarm_lfc_offload_impl(parts, state).await });
}

async fn prewarm_lfc_offload_impl(parts: Parts, AxumState(state): State) -> UnitResult {
    let Parts { uri, token } = parts;
    crate::metrics::LFC_PREWARM_OFFLOAD_REQUESTS.inc();
    info!(%uri, "requesting LFC state from postgres");

    fn internal_err(err: impl std::fmt::Display, msg: &str) {
        error!(%err, msg);
    }

    let mut compressed = Vec::new();
    ComputeNode::get_maintenance_client(&state.tokio_conn_conf)
        .await
        .map_err(|err| error!(%err, "connecting to postgres"))?
        .query_one("select get_local_cache_state()", &[])
        .await
        .map_err(|err| error!(%err, "querying LFC state"))?
        .try_get::<usize, &[u8]>(0)
        .map_err(|err| error!(%err, "deserializing LFC state"))
        .map(ZstdEncoder::new)?
        .read_to_end(&mut compressed)
        .await
        .map_err(|err| error!(%err, "compressing LFC state"))?;
    let compressed_len = compressed.len();
    info!(%uri, "downloaded LFC state, compressed size {compressed_len}, writing to endpoint storage");

    let request = Client::new().put(uri).bearer_auth(token).body(compressed);
    match request.send().await {
        Ok(res) if res.status() == StatusCode::OK => Ok(()),
        Ok(res) => Err(internal_err(res.status(), "writing to endpoint storage")),
        Err(err) => Err(error!(%err, "writing to endpoint storage")),
    }
}

pub async fn prewarm_lfc(parts: Parts, state: State) -> Response {
    {
        let status = &mut state.state.lock().unwrap().prewarm_state.status;
        if *status == Prewarming {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        *status = Prewarming;
    }
    spawn(async move { prewarm_lfc_impl(parts, state).await });
    StatusCode::OK.into_response()
}

fn prewarm_err(state: &ComputeNode, err: impl std::fmt::Display, msg: &str) {
    error!(%err, msg);
    let state = &mut state.state.lock().unwrap().prewarm_state;
    state.status = Failed;
    state.error = err.to_string();
}

async fn prewarm_lfc_impl(parts: Parts, AxumState(state): State) -> UnitResult {
    crate::metrics::LFC_PREWARM_REQUESTS.inc();
    let Parts { uri, token } = parts;
    info!(%uri, "requesting LFC state from endpoint storage");

    let request = Client::new().get(uri.clone()).bearer_auth(token);
    let mut uncompressed = Vec::new();

    let res = match request.send().await {
        Ok(res) => res,
        Err(e) => return Err(prewarm_err(&state, e, "querying endpoint storage")),
    };
    let status = res.status();
    if status != StatusCode::OK {
        return Err(prewarm_err(&state, status, "querying endpoint storage"));
    }

    let lfc_state = res
        .bytes()
        .await
        .map_err(|e| prewarm_err(&state, e, "getting request body from endpoint storage"))?;
    ZstdDecoder::new(lfc_state.iter().as_slice())
        .read_to_end(&mut uncompressed)
        .await
        .map_err(|e| prewarm_err(&state, e, "decoding LFC state"))?;
    let uncompressed_len = uncompressed.len();
    info!(%uri, "downloaded LFC state, uncompressed size {uncompressed_len}, loading into postgres");

    ComputeNode::get_maintenance_client(&state.tokio_conn_conf)
        .await
        .map_err(|e| prewarm_err(&state, e, "connecting to postgres"))?
        .query_one("select prewarm_local_cache($1)", &[&uncompressed])
        .await
        .map_err(|e| prewarm_err(&state, e, "loading LFC state into postgres"))?;

    state.state.lock().unwrap().prewarm_state.status = Completed;
    Ok(())
}

#[derive(Serialize)]
pub(in crate::http) struct PrewarmState {
    status: PrewarmStatus,
    error: String,
    segments: i32,
    done: i32,
}

pub(in crate::http) async fn prewarm_lfc_status(
    state: State,
) -> Result<Json<PrewarmState>, StatusCode> {
    info!("requesting LFC prewarm status from postgres");
    let (status, error);
    {
        let prewarm_state = &state.state.lock().unwrap().prewarm_state;
        status = prewarm_state.status;
        error = prewarm_state.error.clone();
    }

    fn internal_err(err: impl std::fmt::Display, msg: &str) -> StatusCode {
        error!(%err, msg);
        StatusCode::INTERNAL_SERVER_ERROR
    }

    let row = ComputeNode::get_maintenance_client(&state.tokio_conn_conf)
        .await
        .map_err(|e| internal_err(e, "connecting to postgres"))?
        .query_one("select * from get_prewarm_info()", &[])
        .await
        .map_err(|e| internal_err(e, "querying LFC prewarm status"))?;
    let done: i32 = row
        .try_get(0)
        .map_err(|e| internal_err(e, "deserializing LFC prewarm status (done)"))?;
    let segments: i32 = row
        .try_get(1)
        .map_err(|e| internal_err(e, "deserializing LFC prewarm status (segments)"))?;

    let prewarm_state = PrewarmState {
        status,
        error,
        segments,
        done,
    };
    Ok(Json(prewarm_state))
}
