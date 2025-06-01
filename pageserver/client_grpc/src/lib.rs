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
use futures::{Stream, StreamExt};
use thiserror::Error;
use tonic::metadata::AsciiMetadataValue;

use pageserver_page_api::proto;
use pageserver_page_api::*;

use pageserver_page_api::proto::PageServiceClient;
use utils::shard::ShardIndex;

use std::fmt::Debug;
pub mod client_cache;
pub mod request_tracker;
pub mod moc;

use metrics::{IntCounterVec, core::Collector};
use crate::client_cache::{ConnectionPool, PooledItemFactory};

use tokio::sync::mpsc;
use tonic::{transport::{Channel}, Request};
use async_trait::async_trait;

use tokio_stream::wrappers::ReceiverStream;

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

#[derive(Clone, Debug)]
pub struct PageserverClientAggregateMetrics {
    pub request_counters: IntCounterVec,
    pub retry_counters: IntCounterVec,
}
impl PageserverClientAggregateMetrics {
    pub fn new() -> Self {
        let request_counters = IntCounterVec::new(
            metrics::core::Opts::new(
                "backend_requests_total",
                "Number of requests from backends.",
            ),
            &["request_kind"],
        )
        .unwrap();

        let retry_counters = IntCounterVec::new(
            metrics::core::Opts::new(
                "backend_requests_retries_total",
                "Number of retried requests from backends.",
            ),
            &["request_kind"],
        )
        .unwrap();
        Self {
            request_counters,
            retry_counters,
        }
    }

    pub fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        let mut metrics = Vec::new();
        metrics.append(&mut self.request_counters.collect());
        metrics.append(&mut self.retry_counters.collect());
        metrics
    }
}


pub struct StreamFactory {
    connection_pool: Arc<client_cache::ConnectionPool<Channel>>,
    receiver_channel: tokio::sync::mpsc::Sender<proto::GetPageResponse>,
    auth_interceptor: AuthInterceptor,
    shard: ShardIndex,
}

impl StreamFactory {
    pub fn new(
        connection_pool: Arc<ConnectionPool<Channel>>,
        receiver_channel: tokio::sync::mpsc::Sender<proto::GetPageResponse>,
        auth_interceptor: AuthInterceptor,
        shard: ShardIndex,
    ) -> Self {
        StreamFactory {
            connection_pool,
            receiver_channel,
            auth_interceptor,
            shard,
        }
    }
}

#[async_trait]
impl PooledItemFactory<tokio::sync::mpsc::Sender<proto::GetPageRequest>> for StreamFactory {
    async fn create(&self, _connect_timeout: Duration) -> Result<Result<tokio::sync::mpsc::Sender<proto::GetPageRequest>, tonic::Status>, tokio::time::error::Elapsed> {
        let pool_clone : Arc<ConnectionPool<Channel>> = Arc::clone(&self.connection_pool);
        let pooled_client = pool_clone.get_client().await;
        let channel = pooled_client.unwrap().channel();
        let mut client =
            PageServiceClient::with_interceptor(channel, self.auth_interceptor.for_shard(self.shard));

        let (tx, rx) = mpsc::channel::<proto::GetPageRequest>(8);
        let outbound = ReceiverStream::new(rx);

        let client_resp = client
            .get_pages(Request::new(outbound))
            .await;
        match client_resp {
            Err(status) => {
                // TODO: Convert this error correctly
                Ok(Err(tonic::Status::new(
                    status.code(),
                    format!("Failed to connect to pageserver: {}", status.message()),
                )))
            }
            Ok(resp) => {
                let mut response = resp.into_inner();
                let receiver_st = self.receiver_channel.clone();

                // Send all the responses to the channel specified
                let forward_responses = async move {
                    while let Some(r) = response.next().await {
                        match r {
                            Ok(page_response) => {
                                receiver_st.send(page_response).await.unwrap();
                            }
                            Err(status) => {
                                return Err(PageserverClientError::RequestError(status));
                            }
                        }
                    }
                    Ok(())
                };

                // tokio spawn forward_reponses
                tokio::spawn(forward_responses);
                Ok(Ok(tx))
            }
        }
    }
}
pub struct PageserverClient {
    _tenant_id: String,
    _timeline_id: String,

    _auth_token: Option<String>,

    shard_map: HashMap<ShardIndex, String>,

    channels: RwLock<HashMap<ShardIndex, Arc<client_cache::ConnectionPool<Channel>>>>,

    auth_interceptor: AuthInterceptor,

    client_cache_options: ClientCacheOptions,

    aggregate_metrics: Option<Arc<PageserverClientAggregateMetrics>>,
}
#[derive(Clone)]
pub struct ClientCacheOptions {
    pub max_consumers: usize,
    pub error_threshold: usize,
    pub connect_timeout: Duration,
    pub connect_backoff: Duration,
    pub max_idle_duration: Duration,
    pub max_total_connections: usize,
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
            max_consumers: 5000,
            error_threshold: 5,
            connect_timeout: Duration::from_secs(5),
            connect_backoff: Duration::from_secs(1),
            max_idle_duration: Duration::from_secs(60),
            max_total_connections: 100000,
            max_delay_ms: 0,
            drop_rate: 0.0,
            hang_rate: 0.0,
        };
        Self::new_with_config(tenant_id, timeline_id, auth_token, shard_map, options, None)
    }
    pub fn new_with_config(
        tenant_id: &str,
        timeline_id: &str,
        auth_token: &Option<String>,
        shard_map: HashMap<ShardIndex, String>,
        options: ClientCacheOptions,
        metrics: Option<Arc<PageserverClientAggregateMetrics>>,
    ) -> Self {
        Self {
            _tenant_id: tenant_id.to_string(),
            _timeline_id: timeline_id.to_string(),
            _auth_token: auth_token.clone(),
            shard_map,
            channels: RwLock::new(HashMap::new()),
            auth_interceptor: AuthInterceptor::new(tenant_id, timeline_id, auth_token.as_deref()),
            client_cache_options: options,
            aggregate_metrics: metrics,
        }
    }
    pub async fn process_check_rel_exists_request(
        &self,
        request: CheckRelExistsRequest,
    ) -> Result<bool, PageserverClientError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::CheckRelExistsRequest::from(request);
        let response = client.check_rel_exists(tonic::Request::new(request)).await;

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

    pub async fn process_get_rel_size_request(
        &self,
        request: GetRelSizeRequest,
    ) -> Result<u32, PageserverClientError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::GetRelSizeRequest::from(request);
        let response = client.get_rel_size(tonic::Request::new(request)).await;

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

    // Request a single batch of pages
    //
    // TODO: This opens a new gRPC stream for every request, which is extremely inefficient
    pub async fn get_page(
        &self,
        request: GetPageRequest,
    ) -> Result<Vec<Bytes>, PageserverClientError> {
        // FIXME: calculate the shard number correctly
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

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

        match self.aggregate_metrics {
            Some(ref metrics) => {
                metrics
                    .request_counters
                    .with_label_values(&["get_page"])
                    .inc();
            }
            None => {}
        }

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                pooled_client.finish(Ok(())).await; // Pass success to finish
                let response: GetPageResponse = resp.into();
                return Ok(response.page_images.to_vec());
            }
        }
    }

    // Open a stream for requesting pages
    //
    // TODO: This is a pretty low level interface, the caller should not need to be concerned
    // with streams. But 'get_page' is currently very naive and inefficient.
    pub async fn get_pages(
        &self,
        requests: impl Stream<Item = proto::GetPageRequest> + Send + 'static,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<proto::GetPageResponse>>,
        PageserverClientError,
    > {
        // FIXME: calculate the shard number correctly
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let response = client.get_pages(tonic::Request::new(requests)).await;

        match response {
            Err(status) => {
                pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                return Err(PageserverClientError::RequestError(status));
            }
            Ok(resp) => {
                return Ok(resp);
            }
        }
    }

    /// Process a request to get the size of a database.
    pub async fn process_get_dbsize_request(
        &self,
        request: GetDbSizeRequest,
    ) -> Result<u64, PageserverClientError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard = ShardIndex::unsharded();
        let pooled_client = self.get_client(shard).await;
        let chan = pooled_client.channel();

        let mut client =
            PageServiceClient::with_interceptor(chan, self.auth_interceptor.for_shard(shard));

        let request = proto::GetDbSizeRequest::from(request);
        let response = client.get_db_size(tonic::Request::new(request)).await;

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
        request: GetBaseBackupRequest,
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
    async fn get_client(&self, shard: ShardIndex) -> client_cache::PooledClient<Channel> {
        let reused_pool: Option<Arc<client_cache::ConnectionPool<Channel>>> = {
            let channels = self.channels.read().unwrap();
            channels.get(&shard).cloned()
        };

        let usable_pool: Arc<client_cache::ConnectionPool<Channel>>;
        match reused_pool {
            Some(pool) => {
                let pooled_client = pool.get_client().await.unwrap();
                return pooled_client;
            }
            None => {
                // Create a new pool using client_cache_options
                // declare new_pool

                let new_pool: Arc<client_cache::ConnectionPool<Channel>>;
                let channel_fact = Arc::new(client_cache::ChannelFactory::new(
                    self.shard_map.get(&shard).unwrap().clone(),
                    self.client_cache_options.max_delay_ms,
                    self.client_cache_options.drop_rate,
                    self.client_cache_options.hang_rate,
                ));
                new_pool = client_cache::ConnectionPool::new(
                    channel_fact,
                    self.client_cache_options.connect_timeout,
                    self.client_cache_options.connect_backoff,
                    self.client_cache_options.max_consumers,
                    self.client_cache_options.error_threshold,
                    self.client_cache_options.max_idle_duration,
                    self.client_cache_options.max_total_connections,
                    self.aggregate_metrics.clone(),
                );
                let mut write_pool = self.channels.write().unwrap();
                write_pool.insert(shard, new_pool.clone());
                usable_pool = new_pool.clone();
            }
        }

        let pooled_client = usable_pool.get_client().await.unwrap();
        return pooled_client;
    }
}

/// Inject tenant_id, timeline_id and authentication token to all pageserver requests.
#[derive(Clone)]
pub struct AuthInterceptor {
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
