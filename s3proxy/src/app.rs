use anyhow::{Context, Result};
use axum::body::{Body, BodyDataStream};
use axum::response::{IntoResponse, Response};
use axum::{Router, http::StatusCode};
use remote_storage::{DownloadError, DownloadOpts, GenericRemoteStorage, RemotePath};
use s3proxy::{PrefixS3Path, Proxy, S3Path};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub fn app(state: Arc<Proxy>) -> Router<()> {
    use axum::routing::{delete as _delete, get as _get};
    let delete_prefix = _delete(async |p: PrefixS3Path, s: State| merge(delete_prefix(p, s).await));
    let router = Router::new()
        .route(
            "/{tenant_id}/{timeline_id}/{endpoint_id}/{*path}",
            _get(async |p: S3Path, s: State| merge(get(p, s).await))
            .put(async |p: S3Path, s: State, b: Body| merge(set(p, s, b).await))
            .delete(async |p: S3Path, s: State| merge(delete(p, s).await)),
        )
        .route("/{tenant_id}/{timeline_id}/{endpoint_id}", delete_prefix.clone())
        .route("/{tenant_id}/{timeline_id}", delete_prefix.clone())
        .route("/{tenant_id}", delete_prefix)
        // make linter happy
        ;
    // axum-prometheus doesn't work with tokio tests
    // "Failed to set global recorder: SetRecorderError"
    if cfg!(test) {
        return router.with_state(state);
    }
    // TODO add remote_storage/metrics/bucket_metrics?
    let (layer, handle) = axum_prometheus::PrometheusMetricLayer::pair();
    router
        .layer(layer)
        .route("/metrics", _get(|| async move { handle.render() }))
        .with_state(state)
}

type State = axum::extract::State<Arc<Proxy>>;

const CONTENT_TYPE: &str = "content-type";
const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";
async fn get(S3Path { path }: S3Path, state: State) -> Result<Response, Response> {
    info!(%path, "Downloading");
    let download_err = |err| {
        error!(%path, %err, "Error downloading");
        match err {
            DownloadError::NotFound => not_found(&path),
            _ => internal_error(),
        }
    };
    let stream = state
        .storage
        .download(&path, &DownloadOpts::default(), &state.cancel.clone())
        .await
        .map_err(download_err)?
        .download_stream;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        .body(Body::from_stream(stream))
        .map_err(|_| internal_error())
}

async fn set(path: S3Path, state: State, body: Body) -> Result<Response, Response> {
    let S3Path { path } = path;
    info!(%path, "Uploading");
    let upload_err = |err| {
        error!(%path, %err, "Error uploading");
        internal_error()
    };
    use axum::body::HttpBody;
    let stream = body.into_data_stream();
    // TODO Get stream hint size without reading it into Bytes
    let body_size = stream.size_hint().upper().unwrap_or(0) as usize;
    let stream = body_stream_to_sync_stream(stream);
    state
        .storage
        .upload(stream, body_size, &path, None, &state.cancel.clone())
        .await
        .map_err(upload_err)?;
    Ok(ok())
}

async fn delete(S3Path { path }: S3Path, state: State) -> Result<Response, Response> {
    debug!(%path, "Deleting");
    let delete_err = |err| {
        error!(%path, %err, "Error deleting");
        internal_error()
    };
    state
        .storage
        .delete(&path, &state.cancel.clone())
        .await
        .map_err(delete_err)?;
    Ok(ok())
}

async fn delete_prefix(
    PrefixS3Path { path }: PrefixS3Path,
    state: State,
) -> Result<Response, Response> {
    debug!(%path, "Deleting prefix");
    let delete_err = |err| {
        error!(%path, %err, "Error deleting prefix");
        internal_error()
    };
    state
        .storage
        .delete_prefix(&path, &state.cancel.clone())
        .await
        .map_err(delete_err)?;
    Ok(ok())
}

pub async fn check_storage_permissions(
    client: &GenericRemoteStorage,
    cancel: CancellationToken,
) -> Result<()> {
    debug!("Storage permissions check");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_string();

    let path = RemotePath::from_string(&format!("write_access_{now}"))?;
    debug!(%path, "uploading");

    // TODO what if multiple instances try to write a file?
    // random delay before writing? Instances will be ~3 per region + dedicated for computes
    let body = format!("{now}");
    let stream = Body::from(body.clone()).into_data_stream();
    let stream = body_stream_to_sync_stream(stream);
    client
        .upload(stream, body.len(), &path, None, &cancel)
        .await
        .context(format!("uploading {path} to test permissions"))?;

    use tokio::io::AsyncReadExt;
    debug!(%path, "downloading");
    let download_opts = DownloadOpts {
        kind: remote_storage::DownloadKind::Small,
        ..Default::default()
    };
    let mut body_read_buf = Vec::new();
    let stream = client
        .download(&path, &download_opts, &cancel)
        .await
        .context(format!("downloading {path} to test permissions"))?
        .download_stream;
    tokio_util::io::StreamReader::new(stream)
        .read_to_end(&mut body_read_buf)
        .await?;
    let body_read = String::from_utf8(body_read_buf)?;
    if body != body_read {
        error!(%body, %body_read, "File contents do not match");
        anyhow::bail!("Read back file doesn't match original")
    }

    debug!(%path, "removing");
    client.delete(&path, &cancel).await
}

fn merge(res: Result<Response, Response>) -> Response {
    match res {
        Ok(v) => v,
        Err(e) => e,
    }
}

fn ok() -> Response {
    StatusCode::OK.into_response()
}

fn internal_error() -> Response {
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

fn not_found(key: impl ToString) -> Response {
    (StatusCode::NOT_FOUND, key.to_string()).into_response()
}

fn body_stream_to_sync_stream(
    stream: BodyDataStream,
) -> impl futures::Stream<Item = std::io::Result<axum::body::Bytes>> {
    use futures::stream::TryStreamExt;
    sync_wrapper::SyncStream::new(stream)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, extract::Request, response::Response};
    use camino::Utf8PathBuf;
    use itertools::iproduct;
    use std::sync::Arc;
    use tempfile::TempDir;
    use test_log::test as testlog;
    use tower::{Service, util::ServiceExt};

    async fn proxy() -> (Proxy, TempDir) {
        // tests execute in parallel and we need a new directory for each of them
        let dir = tempfile::tempdir().unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let fs = remote_storage::LocalFs::new(path, std::time::Duration::from_secs(5)).unwrap();
        let cancel = CancellationToken::new();
        let proxy = Proxy {
            auth: s3proxy::JwtAuth::new(TEST_PUB_KEY_ED25519).unwrap(),
            storage: GenericRemoteStorage::LocalFs(fs),
            cancel: cancel.clone(),
        };
        check_storage_permissions(&proxy.storage, cancel)
            .await
            .unwrap();
        (proxy, dir)
    }

    // see libs/utils/src/auth.rs
    const TEST_PUB_KEY_ED25519: &[u8] = b"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARYwaNBayR+eGI0iXB4s3QxE3Nl2g1iWbr6KtLWeVD/w=
-----END PUBLIC KEY-----
";

    const TEST_PRIV_KEY_ED25519: &[u8] = br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEID/Drmc1AA6U/znNRWpF3zEGegOATQxfkdWxitcOMsIH
-----END PRIVATE KEY-----
"#;

    async fn request(req: Request<Body>) -> Response<Body> {
        let (proxy, _) = proxy().await;
        app(Arc::new(proxy))
            .into_service()
            .oneshot(req)
            .await
            .unwrap()
    }

    fn routes() -> impl Iterator<Item = (&'static str, &'static str)> {
        iproduct!(
            vec!["/1", "/1/2", "/1/2/3", "/1/2/3/4"],
            vec!["GET", "PUT", "DELETE"]
        )
    }

    #[testlog(tokio::test)]
    async fn no_token() {
        for (uri, method) in routes() {
            info!(%uri, %method);
            let status = Request::builder()
                .uri(uri)
                .method(method)
                .body(Body::empty())
                .map(request)
                .unwrap()
                .await;
            assert!(matches!(
                status.status(),
                StatusCode::METHOD_NOT_ALLOWED | StatusCode::BAD_REQUEST
            ));
        }
    }

    #[testlog(tokio::test)]
    async fn invalid_token() {
        for (uri, method) in routes() {
            info!(%uri, %method);
            let status = Request::builder()
                .uri(uri)
                .header("Authorization", "Bearer 123")
                .method(method)
                .body(Body::empty())
                .map(request)
                .unwrap()
                .await;
            assert!(matches!(
                status.status(),
                StatusCode::METHOD_NOT_ALLOWED | StatusCode::BAD_REQUEST
            ));
        }
    }

    const TENANT_ID: &str = "1adcba3c01c578d1c1be7b8048a4484d";
    const TIMELINE_ID: &str = "16fde223c5e55c4b791e63284681d951";
    const ENDPOINT_ID: &str = "ep-winter-frost-a662z3vg";
    fn token() -> String {
        let claims = s3proxy::Claims {
            tenant_id: TENANT_ID.into(),
            timeline_id: TIMELINE_ID.into(),
            endpoint_id: ENDPOINT_ID.into(),
        };
        let key = jsonwebtoken::EncodingKey::from_ed_pem(TEST_PRIV_KEY_ED25519).unwrap();
        let header = jsonwebtoken::Header::new(s3proxy::VALIDATION_ALGO);
        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    }

    #[testlog(tokio::test)]
    async fn unauthorized() {
        let (proxy, _) = proxy().await;
        let mut app = app(Arc::new(proxy)).into_service();
        let token = token();
        let args = itertools::iproduct!(
            vec![TENANT_ID, "12345"],
            vec![TIMELINE_ID, "12345"],
            vec![ENDPOINT_ID, "ep-ololo"]
        )
        .skip(1);

        for ((uri, method), (tenant, timeline, endpoint)) in iproduct!(routes(), args) {
            info!(%uri, %method, %tenant, %timeline, %endpoint);
            let request = Request::builder()
                .uri(format!("/{tenant}/{timeline}/{endpoint}/sub/path/key"))
                .method(method)
                .header("Authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap();
            let status = ServiceExt::ready(&mut app)
                .await
                .unwrap()
                .call(request)
                .await
                .unwrap()
                .status();
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        }
    }

    #[testlog(tokio::test)]
    async fn method_not_allowed() {
        let token = token();
        let iter = iproduct!(vec!["", "/.."], vec!["GET", "PUT"]);
        for (key, method) in iter {
            let status = Request::builder()
                .uri(format!("/{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}{key}"))
                .method(method)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .map(request)
                .unwrap()
                .await
                .status();
            assert!(matches!(
                status,
                StatusCode::BAD_REQUEST | StatusCode::METHOD_NOT_ALLOWED
            ));
        }
    }

    async fn requests_chain(
        chain: impl Iterator<Item = (&str, &str, &'static str, StatusCode, bool)>,
        token: impl Fn(&str) -> String,
    ) {
        let (proxy, _) = proxy().await;
        let mut app = app(Arc::new(proxy)).into_service();
        for (uri, method, body, expected_status, compare_body) in chain {
            info!(%uri, %method, %body, %expected_status);
            let bearer = format!("Bearer {}", token(uri));
            let request = Request::builder()
                .uri(uri)
                .method(method)
                .header("Authorization", &bearer)
                .body(Body::from(body))
                .unwrap();
            let response = ServiceExt::ready(&mut app)
                .await
                .unwrap()
                .call(request)
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            if !compare_body {
                continue;
            }
            use http_body_util::BodyExt;
            let read_body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, read_body);
        }
    }

    #[testlog(tokio::test)]
    async fn insert_retrieve_remove() {
        let uri = format!("/{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/key");
        let uri = uri.as_str();
        let chain = vec![
            (uri, "GET", "", StatusCode::NOT_FOUND, false),
            (uri, "PUT", "пыщьпыщь", StatusCode::OK, false),
            (uri, "GET", "пыщьпыщь", StatusCode::OK, true),
            (uri, "DELETE", "", StatusCode::OK, false),
            (uri, "GET", "", StatusCode::NOT_FOUND, false),
        ];
        requests_chain(chain.into_iter(), |_| token()).await;
    }

    fn delete_prefix_token(uri: &str) -> String {
        let parts = uri.split("/").collect::<Vec<&str>>();
        let claims = s3proxy::PrefixKeyPath {
            tenant_id: parts.get(1).map(ToString::to_string).unwrap(),
            timeline_id: parts.get(2).map(ToString::to_string).into(),
            endpoint_id: parts.get(3).map(ToString::to_string).into(),
        };
        let key = jsonwebtoken::EncodingKey::from_ed_pem(TEST_PRIV_KEY_ED25519).unwrap();
        let header = jsonwebtoken::Header::new(s3proxy::VALIDATION_ALGO);
        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    }

    #[testlog(tokio::test)]
    async fn delete_prefix() {
        let chain = vec![
            ("/", "DELETE", "", StatusCode::NOT_FOUND, false),
            // create 1/2/3/4, 1/2/3/5, delete prefix 1/2/3 -> empty
            ("/1/2/3/4", "PUT", "", StatusCode::OK, false),
            ("/1/2/3/4", "PUT", "", StatusCode::OK, false), // we can override file contents
            ("/1/2/3/5", "PUT", "", StatusCode::OK, false),
            ("/1/2/3", "DELETE", "", StatusCode::OK, false),
            ("/1/2/3/4", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/2/3/5", "GET", "", StatusCode::NOT_FOUND, false),
            // create 1/2/3/4, 1/2/5/6, delete prefix 1/2/3 -> 1/2/5/6
            ("/1/2/3/4", "PUT", "", StatusCode::OK, false),
            ("/1/2/5/6", "PUT", "", StatusCode::OK, false),
            ("/1/2/3", "DELETE", "", StatusCode::OK, false),
            ("/1/2/3/4", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/2/5/6", "GET", "", StatusCode::OK, false),
            // create 1/2/3/4, 1/2/7/8, delete prefix 1/2 -> empty
            ("/1/2/3/4", "PUT", "", StatusCode::OK, false),
            ("/1/2/7/8", "PUT", "", StatusCode::OK, false),
            ("/1/2", "DELETE", "", StatusCode::OK, false),
            ("/1/2/3/4", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/2/7/8", "GET", "", StatusCode::NOT_FOUND, false),
            // create 1/2/3/4, 1/2/5/6, 1/3/8/9, delete prefix 1/2/3 -> 1/2/5/6, 1/3/8/9
            ("/1/2/3/4", "PUT", "", StatusCode::OK, false),
            ("/1/2/5/6", "PUT", "", StatusCode::OK, false),
            ("/1/3/8/9", "PUT", "", StatusCode::OK, false),
            ("/1/2/3", "DELETE", "", StatusCode::OK, false),
            ("/1/2/3/4", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/2/5/6", "GET", "", StatusCode::OK, false),
            ("/1/3/8/9", "GET", "", StatusCode::OK, false),
            // create 1/4/5/6, delete prefix 1/2 -> 1/3/8/9, 1/4/5/6
            ("/1/4/5/6", "PUT", "", StatusCode::OK, false),
            ("/1/2", "DELETE", "", StatusCode::OK, false),
            ("/1/2/3/4", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/2/5/6", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/3/8/9", "GET", "", StatusCode::OK, false),
            ("/1/4/5/6", "GET", "", StatusCode::OK, false),
            // delete prefix 1 -> empty
            ("/1", "DELETE", "", StatusCode::OK, false),
            ("/1/2/3/4", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/2/5/6", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/3/8/9", "GET", "", StatusCode::NOT_FOUND, false),
            ("/1/4/5/6", "GET", "", StatusCode::NOT_FOUND, false),
        ];
        requests_chain(chain.into_iter(), delete_prefix_token).await;
    }
}
