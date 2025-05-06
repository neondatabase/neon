use crate::compute::ComputeNode;
use anyhow::{Context, Result, bail};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use compute_api::responses::LfcOffloadState;
use compute_api::responses::LfcPrewarmState;
use http::StatusCode;
use reqwest::Client;
use std::sync::Arc;
use tokio::{io::AsyncReadExt, spawn};
use tracing::{error, info};

#[derive(serde::Serialize, Default)]
pub struct LfcPrewarmStateWithProgress {
    #[serde(flatten)]
    base: LfcPrewarmState,
    total: i32,
    prewarmed: i32,
    skipped: i32,
}

/// A pair of url and a token to query endpoint storage for LFC prewarm-related tasks
struct EndpointStoragePair {
    url: String,
    token: String,
}

const KEY: &str = "lfc_state";
impl TryFrom<&crate::compute::ParsedSpec> for EndpointStoragePair {
    type Error = anyhow::Error;
    fn try_from(pspec: &crate::compute::ParsedSpec) -> Result<Self, Self::Error> {
        let Some(ref endpoint_id) = pspec.spec.endpoint_id else {
            bail!("pspec.endpoint_id missing")
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
    // If prewarm failed, we want to get overall number of segments as well as done ones.
    // However, this function should be reliable even if querying postgres failed.
    pub async fn lfc_prewarm_state(&self) -> LfcPrewarmStateWithProgress {
        info!("requesting LFC prewarm state from postgres");
        let mut state = LfcPrewarmStateWithProgress::default();
        {
            state.base = self.state.lock().unwrap().lfc_prewarm_state.clone();
        }

        let client = match ComputeNode::get_maintenance_client(&self.tokio_conn_conf).await {
            Ok(client) => client,
            Err(err) => {
                error!(%err, "connecting to postgres");
                return state;
            }
        };
        let row = match client
            .query_one("select * from get_prewarm_info()", &[])
            .await
        {
            Ok(row) => row,
            Err(err) => {
                error!(%err, "querying LFC prewarm status");
                return state;
            }
        };
        state.total = row.try_get(0).unwrap_or_default();
        state.prewarmed = row.try_get(1).unwrap_or_default();
        state.skipped = row.try_get(2).unwrap_or_default();
        state
    }

    pub fn lfc_offload_state(&self) -> LfcOffloadState {
        self.state.lock().unwrap().lfc_offload_state.clone()
    }

    /// Returns false if there is a prewarm request ongoing, true otherwise
    pub fn prewarm_lfc(self: &Arc<Self>) -> bool {
        crate::metrics::LFC_PREWARM_REQUESTS.inc();
        {
            let state = &mut self.state.lock().unwrap().lfc_prewarm_state;
            if let LfcPrewarmState::Prewarming =
                std::mem::replace(state, LfcPrewarmState::Prewarming)
            {
                return false;
            }
        }

        let cloned = self.clone();
        spawn(async move {
            let Err(err) = cloned.prewarm_impl().await else {
                cloned.state.lock().unwrap().lfc_prewarm_state = LfcPrewarmState::Completed;
                return;
            };
            error!(%err);
            cloned.state.lock().unwrap().lfc_prewarm_state = LfcPrewarmState::Failed {
                error: err.to_string(),
            };
        });
        true
    }

    fn endpoint_storage_pair(&self) -> Result<EndpointStoragePair> {
        let state = self.state.lock().unwrap();
        state.pspec.as_ref().unwrap().try_into()
    }

    async fn prewarm_impl(&self) -> Result<()> {
        let EndpointStoragePair { url, token } = self.endpoint_storage_pair()?;
        info!(%url, "requesting LFC state from endpoint storage");

        let request = Client::new().get(&url).bearer_auth(token);
        let res = request.send().await.context("querying endpoint storage")?;
        let status = res.status();
        if status != StatusCode::OK {
            bail!("{status} querying endpoint storage")
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
        info!(%url, "downloaded LFC state, uncompressed size {uncompressed_len}, loading into postgres");

        ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?
            .query_one("select prewarm_local_cache($1)", &[&uncompressed])
            .await
            .context("loading LFC state into postgres")
            .map(|_| ())
    }

    /// Returns false if there is an offload request ongoing, true otherwise
    pub fn offload_lfc(self: &Arc<Self>) -> bool {
        crate::metrics::LFC_OFFLOAD_REQUESTS.inc();
        {
            let state = &mut self.state.lock().unwrap().lfc_offload_state;
            if let LfcOffloadState::Offloading =
                std::mem::replace(state, LfcOffloadState::Offloading)
            {
                return false;
            }
        }

        let cloned = self.clone();
        spawn(async move {
            let Err(err) = cloned.offload_lfc_impl().await else {
                cloned.state.lock().unwrap().lfc_offload_state = LfcOffloadState::Completed;
                return;
            };
            error!(%err);
            cloned.state.lock().unwrap().lfc_offload_state = LfcOffloadState::Failed {
                error: err.to_string(),
            };
        });
        true
    }

    async fn offload_lfc_impl(&self) -> Result<()> {
        let EndpointStoragePair { url, token } = self.endpoint_storage_pair()?;
        info!(%url, "requesting LFC state from postgres");

        let mut compressed = Vec::new();
        ComputeNode::get_maintenance_client(&self.tokio_conn_conf)
            .await
            .context("connecting to postgres")?
            .query_one("select get_local_cache_state()", &[])
            .await
            .context("querying LFC state")?
            .try_get::<usize, &[u8]>(0)
            .context("deserializing LFC state")
            .map(ZstdEncoder::new)?
            .read_to_end(&mut compressed)
            .await
            .context("compressing LFC state")?;
        let compressed_len = compressed.len();
        info!(%url, "downloaded LFC state, compressed size {compressed_len}, writing to endpoint storage");

        let request = Client::new().put(url).bearer_auth(token).body(compressed);
        match request.send().await {
            Ok(res) if res.status() == StatusCode::OK => Ok(()),
            Ok(res) => bail!("Error writing to endpoint storage: {}", res.status()),
            Err(err) => Err(err).context("writing to endpoint storage"),
        }
    }
}
