use crate::compute::ComputeNode;
use anyhow::{Context, Result, bail};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use compute_api::responses::LfcOffloadState;
use compute_api::responses::LfcPrewarmState;
use http::StatusCode;
use reqwest::Client;
use std::mem::replace;
use std::sync::Arc;
use std::time::Instant;
use tokio::{io::AsyncReadExt, select, spawn};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// A pair of url and a token to query endpoint storage for LFC prewarm-related tasks
struct EndpointStoragePair {
    url: String,
    token: String,
}

const KEY: &str = "lfc_state";
impl EndpointStoragePair {
    /// endpoint_id is set to None while prewarming from other endpoint, see compute_promote.rs
    /// If not None, takes precedence over pspec.spec.endpoint_id
    fn from_spec_and_endpoint(
        pspec: &crate::compute::ParsedSpec,
        endpoint_id: Option<String>,
    ) -> Result<Self> {
        let endpoint_id = endpoint_id.as_ref().or(pspec.spec.endpoint_id.as_ref());
        let Some(ref endpoint_id) = endpoint_id else {
            bail!("pspec.endpoint_id missing, other endpoint_id not provided")
        };
        let Some(ref base_uri) = pspec.endpoint_storage_addr else {
            bail!("pspec.endpoint_storage_addr missing")
        };
        let tenant_id = pspec.tenant_id;
        let timeline_id = pspec.timeline_id;

        let url = format!("http://{base_uri}/{tenant_id}/{timeline_id}/{endpoint_id}/{KEY}");
        let Some(ref token) = pspec.endpoint_storage_token else {
            bail!("pspec.endpoint_storage_token missing")
        };
        let token = token.clone();
        Ok(EndpointStoragePair { url, token })
    }
}

impl ComputeNode {
    pub async fn lfc_prewarm_state(&self) -> LfcPrewarmState {
        self.state.lock().unwrap().lfc_prewarm_state.clone()
    }

    pub fn lfc_offload_state(&self) -> LfcOffloadState {
        self.state.lock().unwrap().lfc_offload_state.clone()
    }

    /// If there is a prewarm request ongoing, return `false`, `true` otherwise.
    /// Has a failpoint "compute-prewarm"
    pub fn prewarm_lfc(self: &Arc<Self>, from_endpoint: Option<String>) -> bool {
        let token: CancellationToken;
        {
            let state = &mut self.state.lock().unwrap();
            token = state.lfc_prewarm_token.clone();
            if let LfcPrewarmState::Prewarming =
                replace(&mut state.lfc_prewarm_state, LfcPrewarmState::Prewarming)
            {
                return false;
            }
        }
        crate::metrics::LFC_PREWARMS.inc();

        let this = self.clone();
        spawn(async move {
            let prewarm_state = match this.prewarm_impl(from_endpoint, token).await {
                Ok(state) => state,
                Err(err) => {
                    crate::metrics::LFC_PREWARM_ERRORS.inc();
                    error!(%err, "could not prewarm LFC");
                    let error = format!("{err:#}");
                    LfcPrewarmState::Failed { error }
                }
            };

            let state = &mut this.state.lock().unwrap();
            if let LfcPrewarmState::Cancelled = prewarm_state {
                state.lfc_prewarm_token = CancellationToken::new();
            }
            state.lfc_prewarm_state = prewarm_state;
        });
        true
    }

    /// from_endpoint: None for endpoint managed by this compute_ctl
    fn endpoint_storage_pair(&self, from_endpoint: Option<String>) -> Result<EndpointStoragePair> {
        let state = self.state.lock().unwrap();
        EndpointStoragePair::from_spec_and_endpoint(state.pspec.as_ref().unwrap(), from_endpoint)
    }

    /// Request LFC state from endpoint storage and load corresponding pages into Postgres.
    async fn prewarm_impl(
        &self,
        from_endpoint: Option<String>,
        token: CancellationToken,
    ) -> Result<LfcPrewarmState> {
        let EndpointStoragePair {
            url,
            token: storage_token,
        } = self.endpoint_storage_pair(from_endpoint)?;

        #[cfg(feature = "testing")]
        fail::fail_point!("compute-prewarm", |_| bail!("compute-prewarm failpoint"));

        info!(%url, "requesting LFC state from endpoint storage");
        let mut now = Instant::now();
        let request = Client::new().get(&url).bearer_auth(storage_token);
        let response = select! {
            _ = token.cancelled() => return Ok(LfcPrewarmState::Cancelled),
            response = request.send() => response
        }
        .context("querying endpoint storage")?;

        match response.status() {
            StatusCode::OK => (),
            StatusCode::NOT_FOUND => return Ok(LfcPrewarmState::Skipped),
            status => bail!("{status} querying endpoint storage"),
        }
        let state_download_time_ms = now.elapsed().as_millis() as u32;
        now = Instant::now();

        let mut uncompressed = Vec::new();
        let lfc_state = select! {
            _ = token.cancelled() => return Ok(LfcPrewarmState::Cancelled),
            lfc_state = response.bytes() => lfc_state
        }
        .context("getting request body from endpoint storage")?;

        let mut decoder = ZstdDecoder::new(lfc_state.iter().as_slice());
        select! {
            _ = token.cancelled() => return Ok(LfcPrewarmState::Cancelled),
            read = decoder.read_to_end(&mut uncompressed) => read
        }
        .context("decoding LFC state")?;
        let uncompress_time_ms = now.elapsed().as_millis() as u32;
        now = Instant::now();

        let uncompressed_len = uncompressed.len();
        info!(%url, "downloaded LFC state, uncompressed size {uncompressed_len}");

        // Client connection and prewarm info querying are fast and therefore don't need
        // cancellation
        let client = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?;
        let pg_token = client.cancel_token();

        let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&uncompressed];
        select! {
            res = client.query_one("select neon.prewarm_local_cache($1)", &params) => res,
            _ = token.cancelled() => {
                pg_token.cancel_query(postgres::NoTls).await
                    .context("cancelling neon.prewarm_local_cache()")?;
                return Ok(LfcPrewarmState::Cancelled)
            }
        }
        .context("loading LFC state into postgres")
        .map(|_| ())?;
        let prewarm_time_ms = now.elapsed().as_millis() as u32;

        let row = client
            .query_one("select * from neon.get_prewarm_info()", &[])
            .await
            .context("querying prewarm info")?;
        let total = row.try_get(0).unwrap_or_default();
        let prewarmed = row.try_get(1).unwrap_or_default();
        let skipped = row.try_get(2).unwrap_or_default();

        Ok(LfcPrewarmState::Completed {
            total,
            prewarmed,
            skipped,
            state_download_time_ms,
            uncompress_time_ms,
            prewarm_time_ms,
        })
    }

    /// If offload request is ongoing, return false, true otherwise
    pub fn offload_lfc(self: &Arc<Self>) -> bool {
        {
            let state = &mut self.state.lock().unwrap().lfc_offload_state;
            if matches!(
                replace(state, LfcOffloadState::Offloading),
                LfcOffloadState::Offloading
            ) {
                return false;
            }
        }
        let cloned = self.clone();
        spawn(async move { cloned.offload_lfc_with_state_update().await });
        true
    }

    pub async fn offload_lfc_async(self: &Arc<Self>) {
        {
            let state = &mut self.state.lock().unwrap().lfc_offload_state;
            if matches!(
                replace(state, LfcOffloadState::Offloading),
                LfcOffloadState::Offloading
            ) {
                return;
            }
        }
        self.offload_lfc_with_state_update().await
    }

    async fn offload_lfc_with_state_update(&self) {
        crate::metrics::LFC_OFFLOADS.inc();
        let state = match self.offload_lfc_impl().await {
            Ok(state) => state,
            Err(err) => {
                crate::metrics::LFC_OFFLOAD_ERRORS.inc();
                error!(%err, "could not offload LFC");
                let error = format!("{err:#}");
                LfcOffloadState::Failed { error }
            }
        };
        self.state.lock().unwrap().lfc_offload_state = state;
    }

    async fn offload_lfc_impl(&self) -> Result<LfcOffloadState> {
        let EndpointStoragePair { url, token } = self.endpoint_storage_pair(None)?;
        info!(%url, "requesting LFC state from Postgres");

        let mut now = Instant::now();
        let row = ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?
            .query_one("select neon.get_local_cache_state()", &[])
            .await
            .context("querying LFC state")?;
        let state = row
            .try_get::<usize, Option<&[u8]>>(0)
            .context("deserializing LFC state")?;
        let Some(state) = state else {
            info!(%url, "empty LFC state, not exporting");
            return Ok(LfcOffloadState::Skipped);
        };
        let state_query_time_ms = now.elapsed().as_millis() as u32;
        now = Instant::now();

        let mut compressed = Vec::new();
        ZstdEncoder::new(state)
            .read_to_end(&mut compressed)
            .await
            .context("compressing LFC state")?;
        let compress_time_ms = now.elapsed().as_millis() as u32;
        now = Instant::now();

        let compressed_len = compressed.len();
        info!(%url, "downloaded LFC state, compressed size {compressed_len}");

        let request = Client::new().put(url).bearer_auth(token).body(compressed);
        let response = request
            .send()
            .await
            .context("writing to endpoint storage")?;
        let state_upload_time_ms = now.elapsed().as_millis() as u32;
        let status = response.status();
        if status != StatusCode::OK {
            bail!("request to endpoint storage failed: {status}");
        }

        Ok(LfcOffloadState::Completed {
            compress_time_ms,
            state_query_time_ms,
            state_upload_time_ms,
        })
    }

    pub fn cancel_prewarm(self: &Arc<Self>) {
        self.state.lock().unwrap().lfc_prewarm_token.cancel();
    }
}
