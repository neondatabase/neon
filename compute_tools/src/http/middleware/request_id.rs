use axum::{extract::Request, middleware::Next, response::Response};
use uuid::Uuid;

use crate::http::headers::X_REQUEST_ID;

/// This middleware function allows compute_ctl to generate its own request ID
/// if one isn't supplied. The control plane will always send one as a UUID. The
/// neon Postgres extension on the other hand does not send one.
pub async fn maybe_add_request_id_header(mut request: Request, next: Next) -> Response {
    let headers = request.headers_mut();
    if !headers.contains_key(X_REQUEST_ID) {
        headers.append(X_REQUEST_ID, Uuid::new_v4().to_string().parse().unwrap());
    }

    next.run(request).await
}
