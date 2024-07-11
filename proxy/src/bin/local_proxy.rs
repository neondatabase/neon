use std::time::Duration;

use proxy::{
    config::HttpConfig,
    serverless::{LocalConnPool, LocalConnPoolOptions},
};

#[tokio::main]
async fn main() {
    let local_pool = LocalConnPool::<tokio_postgres::Client>::new(LocalConnPoolOptions {
        max_conns_per_endpoint: 20,
        idle_timeout: Duration::from_secs(60),
    });
}
