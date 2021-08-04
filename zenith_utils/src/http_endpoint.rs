use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use lazy_static::lazy_static;
use zenith_metrics::{register_int_counter, IntCounter};
use zenith_metrics::{Encoder, TextEncoder};

lazy_static! {
    static ref SERVE_METRICS_COUNT: IntCounter = register_int_counter!(
        "pageserver_serve_metrics_count",
        "Number of metric requests made"
    )
    .expect("failed to define a metric");
}

async fn serve_prometheus_metrics(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    SERVE_METRICS_COUNT.inc();

    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metrics = zenith_metrics::gather();
    encoder.encode(&metrics, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

pub fn thread_main(addr: String) -> anyhow::Result<()> {
    let addr = addr.parse()?;
    log::info!("Starting a prometheus endoint at {}", addr);

    // Enter a single-threaded tokio runtime bound to the current thread
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _guard = runtime.enter();

    // TODO: use hyper_router/routerify/etc when we have more methods
    let server = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, anyhow::Error>(service_fn(serve_prometheus_metrics))
    }));

    runtime.block_on(server)?;

    Ok(())
}
