use std::{sync::Arc, time::Duration};

use proxy::{
    config::{AuthenticationConfig, HttpConfig, ProxyConfig, RetryConfig},
    console::locks::ApiLocks,
    metrics::Metrics,
    rate_limiter::{Aimd, BucketRateLimiter, RateLimiterConfig},
    scram::threadpool::ThreadPool,
    serverless::{
        self, cancel_set::CancelSet, GlobalConnPoolOptions, LocalConnPool, LocalConnPoolOptions,
    },
};

#[tokio::main]
async fn main() {
    let local_pool = LocalConnPool::<tokio_postgres::Client>::new(LocalConnPoolOptions {
        max_conns_per_endpoint: 20,
        idle_timeout: Duration::from_secs(60),
    });

    let config = Box::leak(Box::new(ProxyConfig {
        tls_config: None,
        auth_backend: proxy::auth::BackendType::Local,
        metric_collection: None,
        allow_self_signed_compute: false,
        http_config: HttpConfig {
            pool_options: GlobalConnPoolOptions {
                max_conns_per_endpoint: 20,
                gc_epoch: Duration::from_secs(600),
                pool_shards: 1,
                idle_timeout: Duration::from_secs(30),
                opt_in: false,
                max_total_conns: 1000,
            },
            cancel_set: CancelSet::new(16),
            client_conn_threshold: 100,
        },
        authentication_config: AuthenticationConfig {
            thread_pool: ThreadPool::new(0),
            scram_protocol_timeout: Duration::from_secs(10),
            rate_limiter_enabled: false,
            rate_limiter: BucketRateLimiter::new(vec![]),
            rate_limit_ip_subnet: 64,
        },
        require_client_ip: false,
        handshake_timeout: Duration::from_secs(10),
        region: "local".into(),
        wake_compute_retry_config: RetryConfig::parse(RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)
            .unwrap(),
        connect_compute_locks: ApiLocks::new(
            "connect_compute_lock",
            RateLimiterConfig {
                algorithm: proxy::rate_limiter::RateLimitAlgorithm::Fixed,
                initial_limit: 100,
            },
            1,
            Duration::from_secs(10),
            Duration::from_secs(60),
            &Metrics::get().proxy.connect_compute_lock,
        )
        .unwrap(),
        connect_to_compute_retry_config: RetryConfig::parse(
            RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES,
        )
        .unwrap(),
    }));

    serverless::task_main(
        config,
        ws_listener,
        cancellation_token,
        cancellation_handler,
        endpoint_rate_limiter,
    );

    todo!()
}
