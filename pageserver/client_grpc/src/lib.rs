//! Pageserver Data API client
//!
//! - Manage connections to pageserver
//! - Send requests to correct shards
//!
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use thiserror::Error;
use tonic::metadata::AsciiMetadataValue;

use pageserver_page_api::model::*;
use pageserver_page_api::proto;

use pageserver_page_api::proto::PageServiceClient;
use utils::shard::ShardIndex;

use std::{fmt::Debug};
mod client_cache;
//include info
use tracing::info;

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
}

pub struct PageserverClient {
    _tenant_id: String,
    _timeline_id: String,

    _auth_token: Option<String>,

    shard_map: HashMap<ShardIndex, String>,

    channels: RwLock<HashMap<ShardIndex, Arc<client_cache::ConnectionPool>>>,

    auth_interceptor: AuthInterceptor,

    client_cache_options: ClientCacheOptions,
}

pub struct ClientCacheOptions {
    pub max_consumers: usize,
    pub error_threshold: usize,
    pub connect_timeout: Duration,
    pub connect_backoff: Duration,
    pub max_idle_duration: Duration,
    pub max_delay_ms: u64,
    pub drop_rate: f64,
    pub hang_rate: f64,
}

impl PageserverClient {
    /// TODO: this doesn't currently react to changes in the shard map.
    pub fn new(
        tenant_id: &str,
        timeline_id: &str,
        auth_token: &Option<String>,
        shard_map: HashMap<ShardIndex, String>,
    ) -> Self {
        let options = ClientCacheOptions {
            max_consumers: 10,
            error_threshold: 5,
            connect_timeout: Duration::from_secs(5),
            connect_backoff: Duration::from_secs(1),
            max_idle_duration: Duration::from_secs(60),
            max_delay_ms: 0,
            drop_rate: 0.0,
            hang_rate: 0.0,
        };
        Self::new_with_config(
            tenant_id,
            timeline_id,
            auth_token,
            shard_map,
            options,
        )
    }
    pub fn new_with_config(
        tenant_id: &str,
        timeline_id: &str,
        auth_token: &Option<String>,
        shard_map: HashMap<ShardIndex, String>,
        options: ClientCacheOptions,
    ) -> Self {
        Self {
            _tenant_id: tenant_id.to_string(),
            _timeline_id: timeline_id.to_string(),
            _auth_token: auth_token.clone(),
            shard_map,
            channels: RwLock::new(HashMap::new()),
            auth_interceptor: AuthInterceptor::new(tenant_id, timeline_id, auth_token.as_deref()),
            client_cache_options: options,
        }
    }
    pub async fn process_rel_exists_request(
        &self,
        request: &RelExistsRequest,
    ) -> Result<bool, PageserverClientError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::RelExistsRequest::from(request);
        let response = client.rel_exists(tonic::Request::new(request)).await;

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                return Ok(resp.get_ref().exists);
            }
        }
    }

    pub async fn process_rel_size_request(
        &self,
        request: &RelSizeRequest,
    ) -> Result<u32, PageserverClientError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::RelSizeRequest::from(request);
        let response = client.rel_size(tonic::Request::new(request)).await;

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                return Ok(resp.get_ref().num_blocks);
            }
        }
    }

    pub async fn get_page(&self, request: &GetPageRequest) -> Result<Bytes, PageserverClientError> {
        // FIXME: calculate the shard number correctly
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::GetPageRequest::from(request);
        let response = client.get_page(tonic::Request::new(request)).await;

        match response {
            Err(status) => {
                info!("get_page error: {:?}", status);
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                let response: GetPageResponse = resp.into_inner().try_into()?;
                return Ok(response.page_image);
            }
        }

    }

    // TODO: this should use model::GetPageRequest and GetPageResponse
    pub async fn get_pages(
        &self,
        requests: impl Stream<Item = proto::GetPageRequestBatch> + Send + 'static,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<proto::GetPageResponse>>,
        PageserverClientError,
    > {
        // FIXME: calculate the shard number correctly
        let shard = ShardIndex::unsharded();
        let request = tonic::Request::new(requests);
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let response = client.get_pages(request).await;

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await;
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                return Ok(resp);
            }
        }
    }
    /// Process a request to get the size of a database.
    pub async fn process_dbsize_request(
        &self,
        request: &DbSizeRequest,
    ) -> Result<u64, PageserverClientError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::DbSizeRequest::from(request);
        let response = client.db_size(tonic::Request::new(request)).await;

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                return Ok(resp.get_ref().num_bytes);
            }
        }
    }
    /// Process a request to get the size of a database.
    pub async fn get_base_backup(
        &self,
        request: &GetBaseBackupRequest,
        gzip: bool,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<proto::GetBaseBackupResponseChunk>>,
        PageserverClientError,
    > {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        if gzip {
            client = client.accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        }

        let request = proto::GetBaseBackupRequest::from(request);
        let response = client.get_base_backup(tonic::Request::new(request)).await;

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                return Ok(resp);
            }
        }
    }
    /// Get a client for given shard
    ///
    /// Get a client from the pool for this shard, also creating the pool if it doesn't exist.
    ///
    async fn get_client(&self, shard: ShardIndex) -> client_cache::PooledClient {
        let reused_pool: Option<Arc<client_cache::ConnectionPool>> = {
            let channels = self.channels.read().unwrap();
            channels.get(&shard).cloned()
        };

        let usable_pool: Arc<client_cache::ConnectionPool>;
        match reused_pool {
            Some(pool) => {
                let pooled_client = pool.get_client().await;
                return pooled_client;
            }
            None => {
                // Create a new pool using client_cache_options
                // declare new_pool

                let new_pool: Arc<client_cache::ConnectionPool>;
                new_pool = client_cache::ConnectionPool::new(
                    self.shard_map.get(&shard).unwrap(),
                    self.client_cache_options.max_consumers,
                    self.client_cache_options.error_threshold,
                    self.client_cache_options.connect_timeout,
                    self.client_cache_options.connect_backoff,
                    self.client_cache_options.max_idle_duration,
                    self.client_cache_options.max_delay_ms,
                    self.client_cache_options.drop_rate,
                    self.client_cache_options.hang_rate,
                );
                let mut write_pool = self.channels.write().unwrap();
                write_pool.insert(shard, new_pool.clone());
                usable_pool = new_pool.clone();
            }
        }

        let pooled_client = usable_pool.get_client().await;
        return pooled_client;
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
    fn new(tenant_id: &str, timeline_id: &str, auth_token: Option<&str>) -> Self {
        Self {
            tenant_id: tenant_id.parse().expect("could not parse tenant id"),
            shard_id: None,
            timeline_id: timeline_id.parse().expect("could not parse timeline id"),
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
