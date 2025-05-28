//!
//! Pageserver gRPC client library
//!
//! This library provides a gRPC client for the pageserver for the
//! communicator project.
//!
//! This library is a work in progress.
//!
//!

use std::collections::HashMap;
use bytes::Bytes;
use futures::{StreamExt};
use thiserror::Error;
use tonic::metadata::AsciiMetadataValue;
use pageserver_page_api::model::*;
use pageserver_page_api::proto;
use pageserver_page_api::proto::PageServiceClient;
use utils::shard::ShardIndex;
use std::fmt::Debug;
use tracing::error;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

#[derive(Error, Debug)]
pub enum PageserverClientError {
    #[error("could not connect to service: {0}")]
    ConnectError(#[from] tonic::transport::Error),
    #[error("could not perform request: {0}`")]
    RequestError(#[from] tonic::Status),
    #[error("protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),
    #[error("could not perform request: {0}`")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("could not perform request: {0}`")]
    Other(String),
}

pub struct PageserverClient {
    endpoint_map: HashMap<ShardIndex, Endpoint>,
    channels: tokio::sync::RwLock<HashMap<ShardIndex, Channel>>,
    auth_interceptor: AuthInterceptor,
}

impl PageserverClient {
    /// TODO: this doesn't currently react to changes in the shard map.
    pub fn new(
        tenant_id: AsciiMetadataValue,
        timeline_id: AsciiMetadataValue,
        auth_token: Option<String>,
        shard_map: HashMap<ShardIndex, String>,
    ) -> Result<Self, PageserverClientError> {
        let endpoint_map: HashMap<ShardIndex, Endpoint> = shard_map
            .into_iter()
            .map(|(shard, url)| {
                let endpoint = Endpoint::from_shared(url)
                    .map_err(|_e| PageserverClientError::Other("Unable to parse endpoint {url}".to_string()))?;
                Ok::<(ShardIndex, Endpoint), PageserverClientError>((shard, endpoint))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            endpoint_map,
            channels: RwLock::new(HashMap::new()),
            auth_interceptor: AuthInterceptor::new(
                tenant_id,
                timeline_id,
                auth_token,
            ),
        })
    }
    //
    // TODO: This opens a new gRPC stream for every request, which is extremely inefficient
    pub async fn get_page(
        &self,
        shard: ShardIndex,
        request: &GetPageRequest,
    ) -> Result<Vec<Bytes>, PageserverClientError> {
        // FIXME: calculate the shard number correctly
        let chan = self.get_client(shard).await?;

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::GetPageRequest::from(request);
        let request_stream = futures::stream::once(std::future::ready(request));

        let mut response_stream = client
            .get_pages(tonic::Request::new(request_stream))
            .await?
            .into_inner();

        let Some(response) = response_stream.next().await else {
            return Err(PageserverClientError::Other(
                "no response received for getpage request".to_string(),
            ));
        };

        match response {
            Err(status) => {
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                let response: GetPageResponse = resp.try_into().unwrap();
                return Ok(response.page_images.to_vec());
            }
        }
    }


    //
    // TODO: this should use a connection pool with concurrency limits,
    // not a single connection to the shard.
    //
    async fn get_client(&self, shard: ShardIndex) -> Result<Channel, PageserverClientError> {
        // Get channel from the hashmap
        let mut channels = self.channels.write();
        if let Some(channel) = channels.await.get(&shard) {
            return Ok(channel.clone());
        }
        // Create a new channel if it doesn't exist
        let shard_endpoint = self
            .endpoint_map
            .get(&shard);

        let endpoint = match shard_endpoint{
            Some(_endpoint) => _endpoint,
            None => {
                error!("Shard {shard} not found in shard map");
                return Err(PageserverClientError::Other(format!(
                    "Shard {shard} not found in shard map"
                )));
            }
        };

        let channel = endpoint.connect().await?;
        channels = self.channels.write();
        channels.await.insert(shard, channel.clone());
        Ok(channel.clone())
    }
}

/// Inject tenant_id, timeline_id and authentication token to all pageserver requests.
#[derive(Clone)]
struct AuthInterceptor {
    tenant_id: AsciiMetadataValue,
    shard_id: Option<AsciiMetadataValue>,
    timeline_id: AsciiMetadataValue,
    auth_header: Option<AsciiMetadataValue>, // including "Bearer " prefix
}

impl AuthInterceptor {
    fn new(tenant_id: AsciiMetadataValue,
           timeline_id: AsciiMetadataValue,
           auth_token: Option<String>) -> Self {

        Self {
            tenant_id: tenant_id,
            shard_id: None,
            timeline_id: timeline_id,
            auth_header: auth_token
                .map(|t| format!("Bearer {t}"))
                .map(|t| t.parse().expect("could not parse auth token")),
        }
    }

    fn for_shard(&self, shard_id: ShardIndex) -> Self {
        let mut with_shard = self.clone();
        with_shard.shard_id = Some(
            shard_id
                .to_string()
                .parse()
                .expect("could not parse shard id"),
        );
        with_shard
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut()
            .insert("neon-tenant-id", self.tenant_id.clone());
        if let Some(shard_id) = &self.shard_id {
            req.metadata_mut().insert("neon-shard-id", shard_id.clone());
        }
        req.metadata_mut()
            .insert("neon-timeline-id", self.timeline_id.clone());
        if let Some(auth_header) = &self.auth_header {
            req.metadata_mut()
                .insert("authorization", auth_header.clone());
        }
        Ok(req)
    }
}
