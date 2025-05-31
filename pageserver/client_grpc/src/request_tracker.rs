
//
// API Visible to the spawner, just a function call that is async
//
use std::sync::Arc;
use crate::client_cache;
use pageserver_page_api::model;
use pageserver_page_api::proto;
use crate::client_cache::ConnectionPool;
use crate::AuthInterceptor;
use tonic::transport::Channel;
use crate::ClientCacheOptions;
use crate::PageserverClientAggregateMetrics;
use crate::StreamFactory;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tonic::{
    Status,
    Code,
};

use utils::shard::ShardIndex;

use async_trait::async_trait;
use std::time::Duration;
use client_cache::PooledItemFactory;
use tracing::info;
use tokio::select;
//
// A mock stream pool that just returns a sending channel, and whenever a GetPageRequest
// comes in on that channel, it randomly sleeps before sending a GetPageResponse
//

#[derive(Clone)]
pub struct MockStreamReturner {
    sender: tokio::sync::mpsc::Sender<proto::GetPageRequest>,
    error_returner: tokio::sync::watch::Sender<bool>,
}
pub struct MockStreamFactory {
    receiver_channel: tokio::sync::mpsc::Sender<proto::GetPageResponse>,
}

impl MockStreamFactory {
    pub fn new(
        receiver_channel: tokio::sync::mpsc::Sender<proto::GetPageResponse>,
    ) -> Self {
        MockStreamFactory {
            receiver_channel,
        }
    }
}
#[async_trait]
impl PooledItemFactory<MockStreamReturner> for MockStreamFactory {
    async fn create(&self, _connect_timeout: Duration) -> Result<Result<MockStreamReturner, tonic::Status>, tokio::time::error::Elapsed> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<proto::GetPageRequest>(100);
        let (tx, rx) = tokio::sync::watch::channel::<bool>(false);
        // Create a MockStreamReturner that will send requests to the receiver channel
        let mock_stream_returner = MockStreamReturner {
            sender: sender.clone(),
            error_returner: tx.clone(),
        };

        let receiver_channel = self.receiver_channel.clone();
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {

                // Break out of the loop with 1% chance
                if rand::random::<f32>() < 0.1 {
                    eprintln!("MockStreamReturner: breaking out of the loop");
                    break;
                }
                // Generate a random number between 0 and 100
                // Simulate some processing time
                let receiver_channel_clone = receiver_channel.clone();
                tokio::spawn(async move {
                    let sleep_ms = rand::random::<u64>() % 100;
                    tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                    // Construct a GetPageResponse
                    let response = proto::GetPageResponse {
                        request_id: request.request_id,
                        ..Default::default()
                    };
                    // Send the response back to the receiver channel
                    if let Err(e) = receiver_channel_clone.send(response).await {
                        eprintln!("Failed to send response: {}", e);
                    }
                });
            }
            // broadcast to error_returner
            let _ = tx.send(true);
        });

        Ok(Ok(mock_stream_returner))
    }
}
//
// TODO: This does not handle errors and retries. It also only has one shard.
//
struct Inner {
    current_request_id: u64,
    request_hashmap: std::collections::HashMap<u64, tokio::sync::mpsc::Sender<proto::GetPageResponse>>,
}
#[derive(Clone)]
pub struct RequestTracker {
    inner: Arc<Mutex<Inner>>,
    stream_pool: Arc<ConnectionPool<MockStreamReturner>>,
    client_cache_options: ClientCacheOptions,
}

impl RequestTracker {
    pub fn new(client_cache_options: ClientCacheOptions,
               tenant_id: &str,
               timeline_id: &str,
               auth_token: &Option<String>,
               aggregate_metrics: Arc<PageserverClientAggregateMetrics>,
               endpoint: &str,
    ) -> Self {

        let new_pool: Arc<client_cache::ConnectionPool<Channel>>;
        let channel_fact = Arc::new(client_cache::ChannelFactory::new(
            endpoint.to_string(),
            client_cache_options.max_delay_ms,
            client_cache_options.drop_rate,
            client_cache_options.hang_rate,
        ));
        new_pool = client_cache::ConnectionPool::new(
            channel_fact,
            client_cache_options.connect_timeout,
            client_cache_options.connect_backoff,
            client_cache_options.max_consumers,
            client_cache_options.error_threshold,
            client_cache_options.max_idle_duration,
            Some(Arc::clone(&aggregate_metrics)),
        );
        let mut getpage_response_receiver: tokio::sync::mpsc::Receiver<proto::GetPageResponse>;
        let (getpage_response_sender, mut getpage_response_receiver) =
            tokio::sync::mpsc::channel::<proto::GetPageResponse>(10000);

        let auth_interceptor = AuthInterceptor::new(tenant_id, timeline_id, auth_token.as_deref());

        // Set up a tokio task that listens on the response channel and sends the response
        // to the channel in the hash table that matches that id

        let request_hashmap : std::collections::HashMap<u64, tokio::sync::mpsc::Sender<proto::GetPageResponse>>= std::collections::HashMap::new();
        let inner_data = Arc::new(Mutex::new(Inner {
            current_request_id: 0,
            request_hashmap,
        }));
        let inner_clone = Arc::clone(&inner_data);
        tokio::spawn(async move {
            while let Some(response) = getpage_response_receiver.recv().await {
                // lock inner
                let inner_arc = Arc::clone(&inner_clone);
                let sender : tokio::sync::mpsc::Sender<proto::GetPageResponse>;
                {
                    let mut inner = inner_arc.lock().await;
                    let sender_get = inner.request_hashmap.get(&response.request_id);
                    match sender_get {
                        Some(s) => {
                            // Send the response back to the original request sender
                            sender = s.clone();
                        }
                        None => {
                            eprintln!("No request found for response with ID: {}", response.request_id);
                            continue;
                        }
                    }
                }
                // Send the response to the sender
                if let Err(e) = sender.send(response).await {
                    eprintln!("Failed to send response: {}", e);
                }
            }
        });

        RequestTracker {
            inner: inner_data,
            stream_pool: ConnectionPool::<MockStreamReturner>::new(
                // TODO:
                // Make the mock a parameter to avoid commenting things out
                // Arc::new(StreamFactory::new(new_pool.clone(), getpage_response_sender.clone(),
                //                            auth_interceptor.clone(), ShardIndex::unsharded())),
                Arc::new(MockStreamFactory::new(
                    getpage_response_sender.clone(),
                )),
                client_cache_options.connect_timeout,
                client_cache_options.connect_backoff,
                client_cache_options.max_consumers,
                client_cache_options.error_threshold,
                client_cache_options.max_idle_duration,
                Some(Arc::clone(&aggregate_metrics)),
            ),
            client_cache_options,
        }
    }

    pub async fn send_request(
        &mut self,
        mut request: model::GetPageRequest,
    ) -> proto::GetPageResponse {
        loop {
            // Increment the request ID in a thread-safe manner
            let mut inner = self.inner.lock().await;
            inner.current_request_id += 1;
            let request_id = inner.current_request_id;
            // Explicitly drop the lock before sending the request
            drop(inner);
            let response_sender: tokio::sync::mpsc::Sender<proto::GetPageResponse>;
            let mut response_receiver : tokio::sync::mpsc::Receiver<proto::GetPageResponse>;

            (response_sender, response_receiver) = tokio::sync::mpsc::channel(1);

            // Store the request in the map
            inner = self.inner.lock().await;
            inner.request_hashmap.insert(request_id, response_sender);
            // Ensure the request ID is set in the request
            request.request_id = request_id;
            drop(inner);

            // Get a stream from the stream pool
            let pool_clone = Arc::clone(&self.stream_pool);
            let sender_stream_pool = pool_clone.get_client().await;
            let stream_returner = match sender_stream_pool {
                Ok(stream_ret) => stream_ret,
                Err(e) => {
                    // retry
                    continue;
                }
            };
            let returner = stream_returner.channel();
            let mut rx = returner.error_returner.subscribe();
            let sent = returner.sender.send(proto::GetPageRequest::from(&request))
                .await;

            if let Err(e) = sent {
                // Remove the request from the map if sending failed
                let mut inner = self.inner.lock().await;
                inner.request_hashmap.remove(&request_id);
                drop(inner);
                stream_returner.finish(Err(Status::new(Code::Unknown, "Failed to send request"))).await;
                continue;
            }

            // wait on the response receiver and the tx broadcast at the same time
            select! {
                response = response_receiver.recv() => {
                    if let Some(response) = response {

                        inner = self.inner.lock().await;
                        // Remove the request from the map
                        inner.request_hashmap.remove(&request_id);
                        drop(inner);
                        stream_returner.finish(Result::Ok(())).await;

                        // check that ids are equal
                        if response.request_id != request_id {
                            eprintln!("Response ID {} does not match request ID {}", response.request_id, request_id);
                            continue;
                        };

                        return response.clone();
                    } else {
                        // Handle the case where the response was not received
                        stream_returner.finish(Err(Status::new(Code::Unknown, "Failed to send request"))).await;
                        continue;
                    }
                },
                watch = rx.changed() => {
                    match watch {
                        Ok(_) => {
                            // Remove the request from the map
                            let mut inner = self.inner.lock().await;
                            inner.request_hashmap.remove(&request_id);
                            drop(inner);
                            // loop and try again
                            stream_returner.finish(Err(Status::new(Code::Unknown, "Failed to send request"))).await;
                            continue;
                        },
                        Err(_) => {
                            // If the watch channel is closed, we handle it gracefully
                            stream_returner.finish(Err(Status::new(Code::Unknown, "Failed to send request"))).await;

                        }
                    }
                }
            }
        }
    }
}
