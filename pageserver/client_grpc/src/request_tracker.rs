
//
// API Visible to the spawner, just a function call that is async
//
use std::sync::Arc;
use crate::client_cache;
use pageserver_page_api::model;
use pageserver_page_api::proto;
use crate::client_cache::ConnectionPool;
use crate::AuthInterceptor;
use tonic::{transport::{Channel}, Request};
use crate::ClientCacheOptions;
use crate::PageserverClientAggregateMetrics;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

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
                // Send an empty response to indicate the stream is closed
                let empty_response = proto::GetPageResponse {
                    request_id: 0, // or some other identifier
                    ..Default::default()
                };
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
impl PooledItemFactory<StreamReturner> for StreamFactory {
    async fn create(&self, _connect_timeout: Duration) ->
    Result<Result<StreamReturner, tonic::Status>, tokio::time::error::Elapsed>
    {
        let pool_clone : Arc<ConnectionPool<Channel>> = Arc::clone(&self.connection_pool);
        let pooled_client = pool_clone.get_client().await;
        let channel = pooled_client.unwrap().channel();
        let mut client =
            PageServiceClient::with_interceptor(channel, self.auth_interceptor.for_shard(self.shard));

        let (sender, mut receiver) = tokio::sync::mpsc::channel::<proto::GetPageRequest>(1000);
        let outbound = ReceiverStream::new(receiver);

        let client_resp = client
            .get_pages(Request::new(outbound))
            .await;
        let client_st : &tonic::Response<tonic::Streaming<proto::GetPageResponse>>;
        match client_resp {
            Err(status) => {
                return Ok(Err(tonic::Status::new(
                    status.code(),
                    format!("Failed to open get_page streams: {}", status.message()),
                )));
            }
            Ok(ref client_resp) => {
                client_st = client_resp;
            }
        }

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
                    while let request = inner.message().await {

                        if !request.is_ok() {
                            break; // Exit the loop if no more messages
                        }
                        let req = request.unwrap().unwrap();
                        let response = proto::GetPageResponse {
                            request_id: req.request_id,
                            ..Default::default()
                        };
                        // look up stream in hash map
                        let mut hashmap = map_clone.lock().await;
                        if let Some(sender) = hashmap.get(&req.request_id) {
                            // Send the response to the original request sender
                            if let Err(e) = sender.send(Ok(response.clone())).await {
                                eprintln!("Failed to send response: {}", e);
                            }
                            hashmap.remove(&req.request_id);
                        } else {
                            eprintln!("No sender found for request ID: {}", req.request_id);
                        }
                    }
                    // Close every sender stream in the hashmap
                    let hashmap = map_clone.lock().await;
                    for sender in hashmap.values() {
                        // Send an empty response to indicate the stream is closed
                        let empty_response = proto::GetPageResponse {
                            request_id: 0, // or some other identifier
                            ..Default::default()
                        };
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
//
// TODO: This does not handle errors and retries. It also only has one shard.
//

#[derive(Clone)]
pub struct RequestTracker {
    cur_id: Arc<AtomicU64>,
    stream_pool: Arc<ConnectionPool<StreamReturner>>,
}

impl RequestTracker {
    pub fn new(client_cache_options: ClientCacheOptions,
               tenant_id: &str,
               timeline_id: &str,
               auth_token: &Option<String>,
               aggregate_metrics: Arc<PageserverClientAggregateMetrics>,
               endpoint: &str,
    ) -> Self {

        let _new_pool: Arc<client_cache::ConnectionPool<Channel>>;
        let channel_fact = Arc::new(client_cache::ChannelFactory::new(
            endpoint.to_string(),
            client_cache_options.max_delay_ms,
            client_cache_options.drop_rate,
            client_cache_options.hang_rate,
        ));
        _new_pool = client_cache::ConnectionPool::new(
            channel_fact,
            client_cache_options.connect_timeout,
            client_cache_options.connect_backoff,
            client_cache_options.max_consumers,
            client_cache_options.error_threshold,
            client_cache_options.max_idle_duration,
            client_cache_options.max_total_connections,
            Some(Arc::clone(&aggregate_metrics)),
        );

        let _auth_interceptor = AuthInterceptor::new(tenant_id, timeline_id, auth_token.as_deref());

        // Set up a tokio task that listens on the response channel and sends the response
        // to the channel in the hash table that matches that id

        let cur_id = Arc::new(AtomicU64::new(0));

        RequestTracker {
            cur_id: cur_id.clone(),
            stream_pool: ConnectionPool::<StreamReturner>::new(
                // TODO:
                // Make the mock a parameter to avoid commenting things out
                //Arc::new(StreamFactory::new(new_pool.clone(), getpage_response_sender.clone(),
                 //                           auth_interceptor.clone(), ShardIndex::unsharded())),
                Arc::new(MockStreamFactory::new(
                )),
                client_cache_options.connect_timeout,
                client_cache_options.connect_backoff,
                client_cache_options.max_consumers,
                client_cache_options.error_threshold,
                client_cache_options.max_idle_duration,
                client_cache_options.max_total_connections,
                Some(Arc::clone(&aggregate_metrics)),
            ),
        }
    }

    pub async fn send_request(
        &mut self,
        mut request: model::GetPageRequest,
    ) -> proto::GetPageResponse {
        loop {
            // Increment cur_id
            let request_id = self.cur_id.fetch_add(1, Ordering::SeqCst) + 1;
            let response_sender: tokio::sync::mpsc::Sender<Result<proto::GetPageResponse, Status>>;
            let mut response_receiver: tokio::sync::mpsc::Receiver<Result<proto::GetPageResponse, Status>>;

            (response_sender, response_receiver) = tokio::sync::mpsc::channel(1);
            request.request_id = request_id;

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
            let sent = returner.sender.send(proto::GetPageRequest::from(&request))
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
                            return resp.clone();
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
