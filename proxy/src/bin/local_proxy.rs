use std::time::Duration;

use proxy::{
    config::{AuthenticationConfig, HttpConfig, ProxyConfig, RetryConfig},
    console::locks::ApiLocks,
    serverless::{self, LocalConnPool, LocalConnPoolOptions},
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
            pool_options: (),
            cancel_set: (),
            client_conn_threshold: (),
        },
        authentication_config: AuthenticationConfig {
            thread_pool: todo!(),
            scram_protocol_timeout: todo!(),
            rate_limiter_enabled: todo!(),
            rate_limiter: todo!(),
            rate_limit_ip_subnet: todo!(),
        },
        require_client_ip: false,
        redis_rps_limit: vec![],
        handshake_timeout: Duration::from_secs(10),
        region: "local".into(),
        wake_compute_retry_config: RetryConfig::parse(RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)?,
        connect_compute_locks: ApiLocks::new(name, config, shards, timeout, epoch, metrics),
        connect_to_compute_retry_config: RetryConfig::parse(
            RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES,
        )?,
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
