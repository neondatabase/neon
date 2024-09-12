use quinn::Endpoint;

#[tokio::main]
async fn main() {
    let endpoint: Endpoint = endpoint_config().await.unwrap();

    let quinn_handle = tokio::spawn(quinn_server(endpoint.clone()));

    // tcp listener goes here

    quinn_handle.await.unwrap();
}

async fn endpoint_config() -> anyhow::Result<Endpoint> {
    todo!()
}

async fn quinn_server(_ep: Endpoint) {
    std::future::pending().await
}
