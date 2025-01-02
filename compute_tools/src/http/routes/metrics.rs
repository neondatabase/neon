use axum::{body::Body, response::Response};
use http::header::CONTENT_TYPE;
use http::StatusCode;
use metrics::proto::MetricFamily;
use metrics::Encoder;
use metrics::TextEncoder;

use crate::{http::JsonResponse, installed_extensions};

pub(in crate::http) async fn get_metrics() -> Response {
    let metrics = installed_extensions::collect()
        .into_iter()
        .filter(|m| !m.get_metric().is_empty())
        .collect::<Vec<MetricFamily>>();

    let encoder = TextEncoder::new();
    let mut buffer = vec![];

    if let Err(e) = encoder.encode(&metrics, &mut buffer) {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap()
}
