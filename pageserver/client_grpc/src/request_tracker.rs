
//
// API Visible to the spawner, just a function call that is async
//
use std::sync::Arc;
use crate::client_cache;
use pageserver_page_api::GetPageRequest;
use pageserver_page_api::GetPageResponse;
use pageserver_page_api::*;
use pageserver_page_api::proto;
use crate::client_cache::ConnectionPool;
use crate::client_cache::ChannelFactory;
use crate::AuthInterceptor;
use tonic::{transport::{Channel}, Request};
use crate::ClientCacheOptions;
use crate::PageserverClientAggregateMetrics;
use tokio::sync::Mutex;
use std::sync::atomic::AtomicU64;

use utils::shard::ShardIndex;

use tokio_stream::wrappers::ReceiverStream;
use pageserver_page_api::proto::PageServiceClient;

use tonic::{
    Status,
    Code,
};

use async_trait::async_trait;
use std::time::Duration;

use client_cache::PooledItemFactory;
//use tracing::info;
//
// A mock stream pool that just returns a sending channel, and whenever a GetPageRequest
// comes in on that channel, it randomly sleeps before sending a GetPageResponse
//

#[derive(Clone)]
pub struct StreamReturner {
    sender: tokio::sync::mpsc::Sender<proto::GetPageRequest>,
    sender_hashmap: Arc<Mutex<std::collections::HashMap<u64, tokio::sync::mpsc::Sender<Result<proto::GetPageResponse, Status>>>>>,
}
pub struct MockStreamFactory {
}

impl MockStreamFactory {
    pub fn new() -> Self {
        MockStreamFactory {
        }
    }
}
#[async_trait]
impl PooledItemFactory<StreamReturner> for MockStreamFactory {
    async fn create(&self, _connect_timeout: Duration) -> Result<Result<StreamReturner, tonic::Status>, tokio::time::error::Elapsed> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<proto::GetPageRequest>(1000);
        // Create a StreamReturner that will send requests to the receiver channel
        let stream_returner = StreamReturner {
            sender: sender.clone(),
            sender_hashmap: Arc::new(Mutex::new(std::collections::HashMap::new())),
        };

        let map : Arc<Mutex<std::collections::HashMap<u64, tokio::sync::mpsc::Sender<Result<proto::GetPageResponse, _>>>>>
            = Arc::clone(&stream_returner.sender_hashmap);
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {

                // Break out of the loop with 1% chance
                if rand::random::<f32>() < 0.001 {
                    break;
                }
                // Generate a random number between 0 and 100
                // Simulate some processing time
                let mapclone = Arc::clone(&map);
                tokio::spawn(async move {
                    let sleep_ms = rand::random::<u64>() % 100;
                    tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                    let response = proto::GetPageResponse {
                        request_id: request.request_id,
                        ..Default::default()
                    };
                    // look up stream in hash map
                    let mut hashmap = mapclone.lock().await;
                    if let Some(sender) = hashmap.get(&request.request_id) {
                        // Send the response to the original request sender
                        if let Err(e) = sender.send(Ok(response.clone())).await {
                            eprintln!("Failed to send response: {}", e);
                        }
                        hashmap.remove(&request.request_id);
                    } else {
                        eprintln!("No sender found for request ID: {}", request.request_id);
                    }
                });
            }
            // Close every sender stream in the hashmap
            let hashmap = map.lock().await;
            for sender in hashmap.values() {
                let error = Status::new(Code::Unknown, "Stream closed");
                if let Err(e) = sender.send(Err(error)).await {
                    eprintln!("Failed to send close response: {}", e);
                }
            }
        });

        Ok(Ok(stream_returner))
    }
}


pub struct StreamFactory {
    connection_pool: Arc<client_cache::ConnectionPool<Channel>>,
    auth_interceptor: AuthInterceptor,
    shard: ShardIndex,
}

impl StreamFactory {
    pub fn new(
        connection_pool: Arc<ConnectionPool<Channel>>,
        auth_interceptor: AuthInterceptor,
        shard: ShardIndex,
    ) -> Self {
        StreamFactory {
            connection_pool,
            auth_interceptor,
            shard,
        }
    }
}

#[async_trait]
impl PooledItemFactory<StreamReturner> for StreamFactory {
    async fn create(&self, _connect_timeout: Duration) ->
    Result<Result<StreamReturner, tonic::Status>, tokio::time::error::Elapsed>
    {
        let pool_clone : Arc<ConnectionPool<Channel>> = Arc::clone(&self.connection_pool);
        let pooled_client = pool_clone.get_client().await;
        let channel = pooled_client.unwrap().channel();
        let mut client =
            PageServiceClient::with_interceptor(channel, self.auth_interceptor.for_shard(self.shard));

        let (sender, receiver) = tokio::sync::mpsc::channel::<proto::GetPageRequest>(1000);
        let outbound = ReceiverStream::new(receiver);

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
                let stream_returner = StreamReturner {
                    sender: sender.clone(),
                    sender_hashmap: Arc::new(Mutex::new(std::collections::HashMap::new())),
                };
                let map : Arc<Mutex<std::collections::HashMap<u64, tokio::sync::mpsc::Sender<Result<proto::GetPageResponse, _>>>>>
                    = Arc::clone(&stream_returner.sender_hashmap);

                tokio::spawn(async move {

                    let map_clone = Arc::clone(&map);
                    let mut inner = resp.into_inner();
                    loop {

                        let resp = inner.message().await;
                        if !resp.is_ok() {
                            break; // Exit the loop if no more messages
                        }
                        let response = resp.unwrap().unwrap();

                        // look up stream in hash map
                        let mut hashmap = map_clone.lock().await;
                        if let Some(sender) = hashmap.get(&response.request_id) {
                            // Send the response to the original request sender
                            if let Err(e) = sender.send(Ok(response.clone())).await {
                                eprintln!("Failed to send response: {}", e);
                            }
                            hashmap.remove(&response.request_id);
                        } else {
                            eprintln!("No sender found for request ID: {}", response.request_id);
                        }
                    }
                    // Close every sender stream in the hashmap
                    let hashmap = map_clone.lock().await;
                    for sender in hashmap.values() {
                        let error = Status::new(Code::Unknown, "Stream closed");
                        if let Err(e) = sender.send(Err(error)).await {
                            eprintln!("Failed to send close response: {}", e);
                        }
                    }
                });

                Ok(Ok(stream_returner))
            }
        }
    }
}

#[derive(Clone)]
pub struct RequestTracker {
    _cur_id: Arc<AtomicU64>,
    stream_pool: Arc<ConnectionPool<StreamReturner>>,
    unary_pool: Arc<ConnectionPool<Channel>>,
    auth_interceptor: AuthInterceptor,
    shard: ShardIndex,
}

impl RequestTracker {
    pub fn new(stream_pool: Arc<ConnectionPool<StreamReturner>>,
                unary_pool: Arc<ConnectionPool<Channel>>,
                auth_interceptor: AuthInterceptor,
                shard: ShardIndex,
    ) -> Self {
        let cur_id = Arc::new(AtomicU64::new(0));

        RequestTracker {
            _cur_id: cur_id.clone(),
            stream_pool: stream_pool,
            unary_pool: unary_pool,
            auth_interceptor: auth_interceptor,
            shard: shard.clone()
        }
    }

    pub async fn send_process_check_rel_exists_request(
        &self,
        req: CheckRelExistsRequest,
    ) -> Result<bool, tonic::Status> {
        loop {
            let unary_pool = Arc::clone(&self.unary_pool);
            let pooled_client = unary_pool.get_client().await.unwrap();
            let channel = pooled_client.channel();
            let mut ps_client = PageServiceClient::with_interceptor(channel, self.auth_interceptor.for_shard(self.shard));
            let request = proto::CheckRelExistsRequest::from(req.clone());
            let response = ps_client.check_rel_exists(tonic::Request::new(request)).await;

            match response {
                Err(status) => {
                    pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                    continue;
                }
                Ok(resp) => {
                    pooled_client.finish(Ok(())).await; // Pass success to finish
                    return Ok(resp.get_ref().exists);
                }
            }
        }
    }

    pub async fn send_process_get_rel_size_request(
        &self,
        req: GetRelSizeRequest,
    ) -> Result<u32, tonic::Status> {
        loop {
            // Current sharding model assumes that all metadata is present only at shard 0.
            let unary_pool = Arc::clone(&self.unary_pool);
            let pooled_client = unary_pool.get_client().await.unwrap();
            let channel = pooled_client.channel();
            let mut ps_client = PageServiceClient::with_interceptor(channel, self.auth_interceptor.for_shard(self.shard));

            let request = proto::GetRelSizeRequest::from(req.clone());
            let response = ps_client.get_rel_size(tonic::Request::new(request)).await;

            match response {
                Err(status) => {
                    pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                    continue;
                }
                Ok(resp) => {
                    pooled_client.finish(Ok(())).await; // Pass success to finish
                    return Ok(resp.get_ref().num_blocks);
                }
            }

        }
    }

    pub async fn send_process_get_dbsize_request(
        &self,
        req: GetDbSizeRequest,
    ) -> Result<u64, tonic::Status> {
        loop {
            // Current sharding model assumes that all metadata is present only at shard 0.
            let unary_pool = Arc::clone(&self.unary_pool);
            let pooled_client = unary_pool.get_client().await.unwrap();let channel = pooled_client.channel();
            let mut ps_client = PageServiceClient::with_interceptor(channel, self.auth_interceptor.for_shard(self.shard));

            let request = proto::GetDbSizeRequest::from(req.clone());
            let response = ps_client.get_db_size(tonic::Request::new(request)).await;

            match response {
                Err(status) => {
                    pooled_client.finish(Err(status.clone())).await; // Pass error to finish
                    continue;
                }
                Ok(resp) => {
                    pooled_client.finish(Ok(())).await; // Pass success to finish
                    return Ok(resp.get_ref().num_bytes);
                }
            }

        }
    }

    pub async fn send_getpage_request(
        &mut self,
        req: GetPageRequest,
    ) -> Result<GetPageResponse, tonic::Status> {
        loop {
            let request = req.clone();
            // Increment cur_id
            //let request_id = self.cur_id.fetch_add(1, Ordering::SeqCst) + 1;
            let request_id = request.request_id;
            let response_sender: tokio::sync::mpsc::Sender<Result<proto::GetPageResponse, Status>>;
            let mut response_receiver: tokio::sync::mpsc::Receiver<Result<proto::GetPageResponse, Status>>;

            (response_sender, response_receiver) = tokio::sync::mpsc::channel(1);
            //request.request_id = request_id;

            // Get a stream from the stream pool
            let pool_clone = Arc::clone(&self.stream_pool);
            let sender_stream_pool = pool_clone.get_client().await;
            let stream_returner = match sender_stream_pool {
                Ok(stream_ret) => stream_ret,
                Err(_e) => {
                    // retry
                    continue;
                }
            };
            let returner = stream_returner.channel();
            let map = returner.sender_hashmap.clone();
            // Insert the response sender into the hashmap
            {
                let mut map_inner = map.lock().await;
                map_inner.insert(request_id, response_sender);
            }
            let sent = returner.sender.send(proto::GetPageRequest::from(request))
                .await;

            if let Err(_e) = sent {
                // Remove the request from the map if sending failed
                {
                    let mut map_inner = map.lock().await;
                    // remove from hashmap
                    map_inner.remove(&request_id);
                }
                stream_returner.finish(Err(Status::new(Code::Unknown,
                                                       "Failed to send request"))).await;
                continue;
            }

            let response: Option<Result<proto::GetPageResponse, Status>>;
            response = response_receiver.recv().await;
            match response {
                Some (resp) => {
                    match resp {
                        Err(_status) => {
                            // Handle the case where the response was not received
                            stream_returner.finish(Err(Status::new(Code::Unknown,
                                                                   "Failed to receive response"))).await;
                            continue;
                        },
                        Ok(resp) => {
                            stream_returner.finish(Result::Ok(())).await;
                            return Ok(resp.clone().into());
                        }
                    }
                }
                None => {
                    // Handle the case where the response channel was closed
                    stream_returner.finish(Err(Status::new(Code::Unknown,
                                                           "Response channel closed"))).await;
                    continue;
                }
            }
        }
    }
}

struct ShardedRequestTrackerInner {
    // Hashmap of shard index to RequestTracker
    trackers: std::collections::HashMap<ShardIndex, RequestTracker>,
}
pub struct ShardedRequestTracker {
    inner: Arc<std::sync::Mutex<ShardedRequestTrackerInner>>,
    tcp_client_cache_options: ClientCacheOptions,
    stream_client_cache_options: ClientCacheOptions,
}

//
// TODO: Functions in the ShardedRequestTracker should be able to timeout and
// cancel a reqeust. The request should return an error if it is cancelled.
//
impl ShardedRequestTracker {
    pub fn new() -> Self {
        //
        // Default configuration for the client. These could be added to a config file
        //
        let tcp_client_cache_options = ClientCacheOptions {
            max_delay_ms:       0,
            drop_rate:          0.0,
            hang_rate:          0.0,
            connect_timeout:    Duration::from_secs(1),
            connect_backoff:    Duration::from_millis(100),
            max_consumers:      8, // Streams per connection
            error_threshold:    10,
            max_idle_duration:  Duration::from_secs(5),
            max_total_connections: 8,
        };
        let stream_client_cache_options = ClientCacheOptions {
            max_delay_ms:       0,
            drop_rate:          0.0,
            hang_rate:          0.0,
            connect_timeout:    Duration::from_secs(1),
            connect_backoff:    Duration::from_millis(100),
            max_consumers:      64, // Requests per stream
            error_threshold:    10,
            max_idle_duration:  Duration::from_secs(5),
            max_total_connections: 64, // Total allowable number of streams
        };
        ShardedRequestTracker {
            inner: Arc::new(std::sync::Mutex::new(ShardedRequestTrackerInner {
                trackers: std::collections::HashMap::new(),
            })),
            tcp_client_cache_options,
            stream_client_cache_options,
        }
    }

    pub async fn update_shard_map(&self,
                            shard_urls: std::collections::HashMap<ShardIndex, String>,
                            metrics: Option<Arc<PageserverClientAggregateMetrics>>,
                            tenant_id: String, timeline_id: String, auth_str: Option<&str>) {


       let mut trackers = std::collections::HashMap::new();
        for (shard, endpoint_url) in shard_urls {
            //
            // Create a pool of streams for streaming get_page requests
            //
            let channel_fact : Arc<dyn PooledItemFactory<Channel> + Send + Sync> = Arc::new(ChannelFactory::new(
                endpoint_url.clone(),
                self.tcp_client_cache_options.max_delay_ms,
                self.tcp_client_cache_options.drop_rate,
                self.tcp_client_cache_options.hang_rate,
            ));
            let new_pool: Arc<ConnectionPool<Channel>>;
            new_pool = ConnectionPool::new(
                Arc::clone(&channel_fact),
                self.tcp_client_cache_options.connect_timeout,
                self.tcp_client_cache_options.connect_backoff,
                self.tcp_client_cache_options.max_consumers,
                self.tcp_client_cache_options.error_threshold,
                self.tcp_client_cache_options.max_idle_duration,
                self.tcp_client_cache_options.max_total_connections,
                metrics.clone(),
            );

            let auth_interceptor = AuthInterceptor::new(tenant_id.as_str(),
                                                        timeline_id.as_str(),
                                                        auth_str);

            let stream_pool = ConnectionPool::<StreamReturner>::new(
                Arc::new(StreamFactory::new(new_pool.clone(),
                                            auth_interceptor.clone(), ShardIndex::unsharded())),
                self.stream_client_cache_options.connect_timeout,
                self.stream_client_cache_options.connect_backoff,
                self.stream_client_cache_options.max_consumers,
                self.stream_client_cache_options.error_threshold,
                self.stream_client_cache_options.max_idle_duration,
                self.stream_client_cache_options.max_total_connections,
                metrics.clone(),
            );

            //
            // Create a client pool for unary requests
            //

            let unary_pool: Arc<ConnectionPool<Channel>>;
            unary_pool = ConnectionPool::new(
                Arc::clone(&channel_fact),
                self.tcp_client_cache_options.connect_timeout,
                self.tcp_client_cache_options.connect_backoff,
                self.tcp_client_cache_options.max_consumers,
                self.tcp_client_cache_options.error_threshold,
                self.tcp_client_cache_options.max_idle_duration,
                self.tcp_client_cache_options.max_total_connections,
                metrics.clone()
            );
            //
            // Create a new RequestTracker for this shard
            //
            let new_tracker = RequestTracker::new(stream_pool, unary_pool, auth_interceptor, shard);
            trackers.insert(shard, new_tracker);
        }
        let mut inner = self.inner.lock().unwrap();
        inner.trackers = trackers;
    }

    pub async fn get_page(
        &self,
        req: GetPageRequest,
    ) -> Result<GetPageResponse, tonic::Status> {

        // Get shard index from the request and look up the RequestTracker instance for that shard
        let shard_index = ShardIndex::unsharded(); // TODO!
        let mut tracker = self.lookup_tracker_for_shard(shard_index)?;

        let response = tracker.send_getpage_request(req).await;
        match response {
            Ok(resp) => Ok(resp),
            Err(e) => Err(tonic::Status::unknown(format!("Failed to get page: {}", e))),
        }
    }

    pub async fn process_get_dbsize_request(
        &self,
        request: GetDbSizeRequest,
    ) -> Result<u64, tonic::Status> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let tracker = self.lookup_tracker_for_shard(ShardIndex::unsharded())?;

        let response = tracker.send_process_get_dbsize_request(request).await;
        match response {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    pub async fn process_get_rel_size_request(
        &self,
        request: GetRelSizeRequest,
    ) -> Result<u32, tonic::Status> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let tracker = self.lookup_tracker_for_shard(ShardIndex::unsharded())?;

        let response = tracker.send_process_get_rel_size_request(request).await;
        match response {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    pub async fn process_check_rel_exists_request(
        &self,
        request: CheckRelExistsRequest,
    ) -> Result<bool, tonic::Status> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let tracker = self.lookup_tracker_for_shard(ShardIndex::unsharded())?;

        let response = tracker.send_process_check_rel_exists_request(request).await;
        match response {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    fn lookup_tracker_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> Result<RequestTracker, tonic::Status> {
        let inner = self.inner.lock().unwrap();
        if let Some(t) = inner.trackers.get(&shard_index) {
            Ok(t.clone())
        } else {
            Err(tonic::Status::not_found(format!(
                "Shard {} not found",
                shard_index
            )))
        }
    }
}
