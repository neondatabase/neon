use crate::compute::ComputeNode;
use anyhow::Context;
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use axum::response::{IntoResponse, Response};
use axum::{Json, http::StatusCode};
use reqwest::Client;
use serde::Serialize;
use std::sync::Arc;
use tokio::{io::AsyncReadExt, spawn};
use tracing::{error, info};

type State = axum::extract::State<Arc<ComputeNode>>;
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

        let Some(ref endpoint_id) = pspec.spec.endpoint_id else {
            return Err("pspec.endpoint_id missing");
        };
        let Some(ref base_uri) = pspec.endpoint_storage_addr else {
            return Err("pspec.endpoint_storage_addr missing");
        };
        let tenant_id = pspec.tenant_id;
        let timeline_id = pspec.timeline_id;

        let uri = format!("{base_uri}/{tenant_id}/{timeline_id}/{endpoint_id}/{KEY}");
        let Some(ref token) = pspec.endpoint_storage_token else {
            return Err("pspec.endpoint_storage_token missing");
        };
        let token = token.clone();
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
    crate::metrics::LFC_PREWARM_OFFLOAD_REQUESTS.inc();
    spawn(async move { prewarm_lfc_offload_impl(parts, state).await });
}

async fn prewarm_lfc_offload_impl(parts: Parts, state: State) -> Result<(), ()> {
    let Parts { uri, token } = parts;
    info!(%uri, "requesting LFC state from postgres");

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
        Ok(res) => {
            let err = res.status().to_string();
            Err(error!(%err, "writing to endpoint storage"))
        }
        Err(err) => Err(error!(%err, "writing to endpoint storage")),
    }
}

pub async fn prewarm_lfc(parts: Parts, state: State) -> Response {
    crate::metrics::LFC_PREWARM_REQUESTS.inc();
    use compute_api::responses::PrewarmStatus::*;
    {
        let status = &mut state.state.lock().unwrap().prewarm_state.status;
        if *status == Prewarming {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        *status = Prewarming;
    }
    spawn(async move {
        let Err(err) = prewarm_lfc_impl(parts, &state.tokio_conn_conf).await else {
            state.state.lock().unwrap().prewarm_state.status = Completed;
            return;
        };
        error!(%err);
        let state = &mut state.state.lock().unwrap().prewarm_state;
        state.status = Failed;
        state.error = err.to_string();
    });
    StatusCode::OK.into_response()
}

async fn prewarm_lfc_impl(parts: Parts, conn: &tokio_postgres::Config) -> anyhow::Result<()> {
    let Parts { uri, token } = parts;
    info!(%uri, "requesting LFC state from endpoint storage");

    let request = Client::new().get(uri.clone()).bearer_auth(token);
    let res = request.send().await.context("querying endpoint storage")?;
    let status = res.status();
    if status != StatusCode::OK {
        anyhow::bail!("{status} querying endpoint storage")
    }

    let mut uncompressed = Vec::new();
    let lfc_state = res
        .bytes()
        .await
        .context("getting request body from endpoint storage")?;
    ZstdDecoder::new(lfc_state.iter().as_slice())
        .read_to_end(&mut uncompressed)
        .await
        .context("decoding LFC state")?;
    let uncompressed_len = uncompressed.len();
    info!(%uri, "downloaded LFC state, uncompressed size {uncompressed_len}, loading into postgres");

    ComputeNode::get_maintenance_client(conn)
        .await
        .context("connecting to postgres")?
        .query_one("select prewarm_local_cache($1)", &[&uncompressed])
        .await
        .context("loading LFC state into postgres")
        .map(|_| ())
}

#[derive(Serialize, Default)]
pub(in crate::http) struct PrewarmStatus {
    status: compute_api::responses::PrewarmStatus,
    error: String,
    segments: i32,
    done: i32,
}

// If prewarm failed, we want to get overall number of segments as well as done ones.
// However, this route should be reliable even if querying postgres failed.
pub(in crate::http) async fn prewarm_lfc_status(state: State) -> Json<PrewarmStatus> {
    info!("requesting LFC prewarm status from postgres");
    let mut status = PrewarmStatus::default();
    {
        let state = &state.state.lock().unwrap().prewarm_state;
        status.status = state.status;
        status.error = state.error.clone();
    }

    let res = match ComputeNode::get_maintenance_client(&state.tokio_conn_conf).await {
        Ok(res) => res,
        Err(err) => {
            error!(%err, "connecting to postgres");
            return Json(status);
        }
    };
    let row = match res.query_one("select * from get_prewarm_info()", &[]).await {
        Ok(row) => row,
        Err(err) => {
            error!(%err, "querying LFC prewarm status");
            return Json(status);
        }
    };
    status.done = row.try_get(0).unwrap_or_default();
    status.segments = row.try_get(1).unwrap_or_default();

    Json(status)
}
