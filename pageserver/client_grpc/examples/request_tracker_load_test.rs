// examples/request_tracker_load_test.rs

use std::{sync::Arc, time::Duration};
use tokio;
use pageserver_client_grpc::request_tracker::RequestTracker;
use pageserver_client_grpc::request_tracker::MockStreamFactory;
use pageserver_client_grpc::request_tracker::StreamReturner;
use pageserver_client_grpc::client_cache::ConnectionPool;
use pageserver_client_grpc::client_cache::PooledItemFactory;
use pageserver_client_grpc::ClientCacheOptions;
use pageserver_client_grpc::PageserverClientAggregateMetrics;
use pageserver_client_grpc::AuthInterceptor;

use pageserver_client_grpc::client_cache::ChannelFactory;

use tonic::transport::Channel;

use rand::prelude::*;

use pageserver_api::key::Key;

use utils::lsn::Lsn;
use utils::shard::ShardIndex;

use futures::stream::FuturesOrdered;
use futures::StreamExt;

use pageserver_page_api::proto;

#[tokio::main]
async fn main() {
    // 1) configure the client‐pool behavior
    let client_cache_options = ClientCacheOptions {
        max_delay_ms:       0,
        drop_rate:          0.0,
        hang_rate:          0.0,
        connect_timeout:    Duration::from_secs(10),
        connect_backoff:    Duration::from_millis(200),
        max_consumers:      64,
        error_threshold:    10,
        max_idle_duration:  Duration::from_secs(60),
        max_total_connections: 12,
    };

    // 2) metrics collector (we assume Default is implemented)
    let metrics = Arc::new(PageserverClientAggregateMetrics::new());
    let pool = ConnectionPool::<StreamReturner>::new(
        Arc::new(MockStreamFactory::new(
        )),
        client_cache_options.connect_timeout,
        client_cache_options.connect_backoff,
        client_cache_options.max_consumers,
        client_cache_options.error_threshold,
        client_cache_options.max_idle_duration,
        client_cache_options.max_total_connections,
        Some(Arc::clone(&metrics)),
    );

    // -----------
    // There is no mock for the unary connection pool, so for now just
    // don't use this pool
    //
    let channel_fact : Arc<dyn PooledItemFactory<Channel> + Send + Sync> = Arc::new(ChannelFactory::new(
        "".to_string(),
        client_cache_options.max_delay_ms,
        client_cache_options.drop_rate,
        client_cache_options.hang_rate,
    ));
    let unary_pool: Arc<ConnectionPool<Channel>> = ConnectionPool::new(
        Arc::clone(&channel_fact),
        client_cache_options.connect_timeout,
        client_cache_options.connect_backoff,
        client_cache_options.max_consumers,
        client_cache_options.error_threshold,
        client_cache_options.max_idle_duration,
        client_cache_options.max_total_connections,
        Some(Arc::clone(&metrics)),
    );

    // -----------
    // Dummy auth interceptor. This is not used in this test.
    let auth_interceptor = AuthInterceptor::new("dummy_tenant_id",
                                                "dummy_timeline_id",
                                                None);
    let tracker = RequestTracker::new(
        pool,
        unary_pool,
        auth_interceptor,
        ShardIndex::unsharded(),
    );

    // 4) fire off 10 000 requests in parallel
    let mut handles = FuturesOrdered::new();
    for _i in 0..500000 {

            let mut rng = rand::thread_rng();
            let r = 0..=1000000i128;
            let key: i128 = rng.gen_range(r.clone());
            let key = Key::from_i128(key);
            let (rel_tag, block_no) = key
                .to_rel_block()
                .expect("we filter non-rel-block keys out above");

            let req2 = proto::GetPageRequest {
                request_id: 0,
                request_class: proto::GetPageClass::Normal as i32,
                read_lsn: Some(proto::ReadLsn {
                    request_lsn: if rng.gen_bool(0.5) {
                        u64::from(Lsn::MAX)
                    } else {
                        10000
                    },
                    not_modified_since_lsn: 10000,
                }),
                rel: Some(rel_tag.into()),
                block_number: vec![block_no],
            };
        let req_model = pageserver_page_api::GetPageRequest::try_from(req2.clone());

        // RequestTracker is Clone, so we can share it
        let mut tr = tracker.clone();
        let fut = async move {
            let resp = tr.send_getpage_request(req_model.unwrap()).await.unwrap();
            // sanity‐check: the mock echo returns the same request_id
            assert!(resp.request_id > 0);
        };
        handles.push_back(fut);

        // empty future
        let fut = async move {};
        fut.await;
    }

    // print timestamp
    println!("Starting 5000000 requests at: {}", chrono::Utc::now());
    // 5) wait for them all
    for _i in 0..500000 {
        handles.next().await.expect("Failed to get next handle");
    }

    // print timestamp
    println!("Finished 5000000 requests at: {}", chrono::Utc::now());

    println!("✅ All 100000 requests completed successfully");
}
