use std::collections::HashMap;

use futures::Future;
use pageserver_api::config::NodeMetadata;
use pageserver_api::controller_api::{AvailabilityZone, NodeRegisterRequest};
use pageserver_api::models::ShardImportStatus;
use pageserver_api::shard::TenantShardId;
use pageserver_api::upcall_api::{
    PutTimelineImportStatusRequest, ReAttachRequest, ReAttachResponse, ReAttachResponseTenant,
    ValidateRequest, ValidateRequestTenant, ValidateResponse,
};
use reqwest::Certificate;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use url::Url;
use utils::generation::Generation;
use utils::id::{NodeId, TimelineId};
use utils::{backoff, failpoint_support};

use crate::config::PageServerConf;
use crate::virtual_file::on_fatal_io_error;

/// The Pageserver's client for using the storage controller upcall API: this is a small API
/// for dealing with generations (see docs/rfcs/025-generation-numbers.md).
pub struct StorageControllerUpcallClient {
    http_client: reqwest::Client,
    base_url: Url,
    node_id: NodeId,
    cancel: CancellationToken,
}

/// Represent operations which internally retry on all errors other than
/// cancellation token firing: the only way they can fail is ShuttingDown.
pub enum RetryForeverError {
    ShuttingDown,
}

pub trait StorageControllerUpcallApi {
    fn re_attach(
        &self,
        conf: &PageServerConf,
    ) -> impl Future<
        Output = Result<HashMap<TenantShardId, ReAttachResponseTenant>, RetryForeverError>,
    > + Send;
    fn validate(
        &self,
        tenants: Vec<(TenantShardId, Generation)>,
    ) -> impl Future<Output = Result<HashMap<TenantShardId, bool>, RetryForeverError>> + Send;
    fn put_timeline_import_status(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        status: ShardImportStatus,
    ) -> impl Future<Output = Result<(), RetryForeverError>> + Send;
    fn get_timeline_import_status(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> impl Future<Output = Result<Option<ShardImportStatus>, RetryForeverError>> + Send;
}

impl StorageControllerUpcallClient {
    /// A None return value indicates that the input `conf` object does not have control
    /// plane API enabled.
    pub fn new(conf: &'static PageServerConf, cancel: &CancellationToken) -> Self {
        let mut url = conf.control_plane_api.clone();

        if let Ok(mut segs) = url.path_segments_mut() {
            // This ensures that `url` ends with a slash if it doesn't already.
            // That way, we can subsequently use join() to safely attach extra path elements.
            segs.pop_if_empty().push("");
        }

        let mut client = reqwest::ClientBuilder::new();

        if let Some(jwt) = &conf.control_plane_api_token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "Authorization",
                format!("Bearer {}", jwt.get_contents()).parse().unwrap(),
            );
            client = client.default_headers(headers);
        }

        for cert in &conf.ssl_ca_certs {
            client = client.add_root_certificate(
                Certificate::from_der(cert.contents()).expect("Invalid certificate in config"),
            );
        }

        Self {
            http_client: client.build().expect("Failed to construct HTTP client"),
            base_url: url,
            node_id: conf.id,
            cancel: cancel.clone(),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn retry_http_forever<R, T>(
        &self,
        url: &url::Url,
        request: R,
    ) -> Result<T, RetryForeverError>
    where
        R: Serialize,
        T: DeserializeOwned,
    {
        let res = backoff::retry(
            || async {
                let response = self
                    .http_client
                    .post(url.clone())
                    .json(&request)
                    .send()
                    .await?;

                response.error_for_status_ref()?;
                response.json::<T>().await
            },
            |_| false,
            3,
            u32::MAX,
            "storage controller upcall",
            &self.cancel,
        )
        .await
        .ok_or(RetryForeverError::ShuttingDown)?
        .expect("We retry forever, this should never be reached");

        Ok(res)
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.base_url
    }
}

impl StorageControllerUpcallApi for StorageControllerUpcallClient {
    /// Block until we get a successful response, or error out if we are shut down
    #[tracing::instrument(skip_all)] // so that warning logs from retry_http_forever have context
    async fn re_attach(
        &self,
        conf: &PageServerConf,
    ) -> Result<HashMap<TenantShardId, ReAttachResponseTenant>, RetryForeverError> {
        let url = self
            .base_url
            .join("re-attach")
            .expect("Failed to build re-attach path");

        // Include registration content in the re-attach request if a metadata file is readable
        let metadata_path = conf.metadata_path();
        let register = match tokio::fs::read_to_string(&metadata_path).await {
            Ok(metadata_str) => match serde_json::from_str::<NodeMetadata>(&metadata_str) {
                Ok(m) => {
                    // Since we run one time at startup, be generous in our logging and
                    // dump all metadata.
                    tracing::info!(
                        "Loaded node metadata: postgres {}:{}, http {}:{}, other fields: {:?}",
                        m.postgres_host,
                        m.postgres_port,
                        m.http_host,
                        m.http_port,
                        m.other
                    );

                    let az_id = {
                        let az_id_from_metadata = m
                            .other
                            .get("availability_zone_id")
                            .and_then(|jv| jv.as_str().map(|str| str.to_owned()));

                        match az_id_from_metadata {
                            Some(az_id) => Some(AvailabilityZone(az_id)),
                            None => {
                                tracing::warn!(
                                    "metadata.json does not contain an 'availability_zone_id' field"
                                );
                                conf.availability_zone.clone().map(AvailabilityZone)
                            }
                        }
                    };

                    if az_id.is_none() {
                        panic!(
                            "Availablity zone id could not be inferred from metadata.json or pageserver config"
                        );
                    }

                    Some(NodeRegisterRequest {
                        node_id: conf.id,
                        listen_pg_addr: m.postgres_host,
                        listen_pg_port: m.postgres_port,
                        listen_http_addr: m.http_host,
                        listen_http_port: m.http_port,
                        listen_https_port: m.https_port,
                        availability_zone_id: az_id.expect("Checked above"),
                    })
                }
                Err(e) => {
                    tracing::error!("Unreadable metadata in {metadata_path}: {e}");
                    None
                }
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // This is legal: we may have been deployed with some external script
                    // doing registration for us.
                    tracing::info!("Metadata file not found at {metadata_path}");
                } else {
                    on_fatal_io_error(&e, &format!("Loading metadata at {metadata_path}"))
                }
                None
            }
        };

        let request = ReAttachRequest {
            node_id: self.node_id,
            register: register.clone(),
        };

        let response: ReAttachResponse = self.retry_http_forever(&url, request).await?;
        tracing::info!(
            "Received re-attach response with {} tenants (node {}, register: {:?})",
            response.tenants.len(),
            self.node_id,
            register,
        );

        failpoint_support::sleep_millis_async!("control-plane-client-re-attach");

        Ok(response
            .tenants
            .into_iter()
            .map(|rart| (rart.id, rart))
            .collect::<HashMap<_, _>>())
    }

    /// Block until we get a successful response, or error out if we are shut down
    #[tracing::instrument(skip_all)] // so that warning logs from retry_http_forever have context
    async fn validate(
        &self,
        tenants: Vec<(TenantShardId, Generation)>,
    ) -> Result<HashMap<TenantShardId, bool>, RetryForeverError> {
        let url = self
            .base_url
            .join("validate")
            .expect("Failed to build validate path");

        // When sending validate requests, break them up into chunks so that we
        // avoid possible edge cases of generating any HTTP requests that
        // require database I/O across many thousands of tenants.
        let mut result: HashMap<TenantShardId, bool> = HashMap::with_capacity(tenants.len());
        for tenant_chunk in (tenants).chunks(128) {
            let request = ValidateRequest {
                tenants: tenant_chunk
                    .iter()
                    .map(|(id, generation)| ValidateRequestTenant {
                        id: *id,
                        r#gen: (*generation).into().expect(
                            "Generation should always be valid for a Tenant doing deletions",
                        ),
                    })
                    .collect(),
            };

            failpoint_support::sleep_millis_async!(
                "control-plane-client-validate-sleep",
                &self.cancel
            );
            if self.cancel.is_cancelled() {
                return Err(RetryForeverError::ShuttingDown);
            }

            let response: ValidateResponse = self.retry_http_forever(&url, request).await?;
            for rt in response.tenants {
                result.insert(rt.id, rt.valid);
            }
        }

        Ok(result.into_iter().collect())
    }

    /// Send a shard import status to the storage controller
    ///
    /// The implementation must have at-least-once delivery semantics.
    /// To this end, we retry the request until it succeeds. If the pageserver
    /// restarts or crashes, the shard import will start again from the beggining.
    #[tracing::instrument(skip_all)] // so that warning logs from retry_http_forever have context
    async fn put_timeline_import_status(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        status: ShardImportStatus,
    ) -> Result<(), RetryForeverError> {
        let url = self
            .base_url
            .join("timeline_import_status")
            .expect("Failed to build path");

        let request = PutTimelineImportStatusRequest {
            tenant_shard_id,
            timeline_id,
            status,
        };

        self.retry_http_forever(&url, request).await
    }

    #[tracing::instrument(skip_all)] // so that warning logs from retry_http_forever have context
    async fn get_timeline_import_status(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<Option<ShardImportStatus>, RetryForeverError> {
        let url = self
            .base_url
            .join(format!("timeline_import_status/{}/{}", tenant_shard_id, timeline_id).as_str())
            .expect("Failed to build path");

        Ok(backoff::retry(
            || async {
                let response = self.http_client.get(url.clone()).send().await?;

                if let Err(err) = response.error_for_status_ref() {
                    if matches!(err.status(), Some(reqwest::StatusCode::NOT_FOUND)) {
                        return Ok(None);
                    } else {
                        return Err(err);
                    }
                }
                response.json::<ShardImportStatus>().await.map(Some)
            },
            |_| false,
            3,
            u32::MAX,
            "storage controller upcall",
            &self.cancel,
        )
        .await
        .ok_or(RetryForeverError::ShuttingDown)?
        .expect("We retry forever, this should never be reached"))
    }
}
