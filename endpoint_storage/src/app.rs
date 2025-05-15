use anyhow::anyhow;
use axum::body::{Body, Bytes};
use axum::response::{IntoResponse, Response};
use axum::{Router, http::StatusCode};
use endpoint_storage::{PrefixS3Path, S3Path, Storage, bad_request, internal_error, not_found, ok};
use remote_storage::TimeoutOrCancel;
use remote_storage::{DownloadError, DownloadOpts, GenericRemoteStorage, RemotePath};
use std::{sync::Arc, time::SystemTime, time::UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use utils::backoff::retry;

pub fn app(state: Arc<Storage>) -> Router<()> {
    use axum::routing::{delete as _delete, get as _get};
    let delete_prefix = _delete(delete_prefix);
    Router::new()
        .route(
            "/{tenant_id}/{timeline_id}/{endpoint_id}/{*path}",
            _get(get).put(set).delete(delete),
        )
        .route(
            "/{tenant_id}/{timeline_id}/{endpoint_id}",
            delete_prefix.clone(),
        )
        .route("/{tenant_id}/{timeline_id}", delete_prefix.clone())
        .route("/{tenant_id}", delete_prefix)
        .route("/metrics", _get(metrics))
        .route("/status", _get(async || StatusCode::OK.into_response()))
        .with_state(state)
}

type Result = anyhow::Result<Response, Response>;
type State = axum::extract::State<Arc<Storage>>;

const CONTENT_TYPE: &str = "content-type";
const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";
const WARN_THRESHOLD: u32 = 3;
const MAX_RETRIES: u32 = 10;

async fn metrics() -> Result {
    prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .map(|s| s.into_response())
        .map_err(|e| internal_error(e, "/metrics", "collecting metrics"))
}

async fn get(S3Path { path }: S3Path, state: State) -> Result {
    info!(%path, "downloading");
    let download_err = |err| {
        if let DownloadError::NotFound = err {
            info!(%path, %err, "downloading"); // 404 is not an issue of _this_ service
            return not_found(&path);
        }
        internal_error(err, &path, "downloading")
    };
    let cancel = state.cancel.clone();
    let opts = &DownloadOpts::default();

    let stream = retry(
        async || state.storage.download(&path, opts, &cancel).await,
        DownloadError::is_permanent,
        WARN_THRESHOLD,
        MAX_RETRIES,
        "downloading",
        &cancel,
    )
    .await
    .unwrap_or(Err(DownloadError::Cancelled))
    .map_err(download_err)?
    .download_stream;

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        .body(Body::from_stream(stream))
        .map_err(|e| internal_error(e, path, "reading response"))
}

// Best solution for files is multipart upload, but remote_storage doesn't support it,
// so we can either read Bytes in memory and push at once or forward BodyDataStream to
// remote_storage. The latter may seem more peformant, but BodyDataStream doesn't have a
// guaranteed size() which may produce issues while uploading to s3.
// So, currently we're going with an in-memory copy plus a boundary to prevent uploading
// very large files.
async fn set(S3Path { path }: S3Path, state: State, bytes: Bytes) -> Result {
    info!(%path, "uploading");
    let request_len = bytes.len();
    let max_len = state.max_upload_file_limit;
    if request_len > max_len {
        return Err(bad_request(
            anyhow!("File size {request_len} exceeds max {max_len}"),
            "uploading",
        ));
    }

    let cancel = state.cancel.clone();
    let fun = async || {
        let stream = bytes_to_stream(bytes.clone());
        state
            .storage
            .upload(stream, request_len, &path, None, &cancel)
            .await
    };
    retry(
        fun,
        TimeoutOrCancel::caused_by_cancel,
        WARN_THRESHOLD,
        MAX_RETRIES,
        "uploading",
        &cancel,
    )
    .await
    .unwrap_or(Err(anyhow!("uploading cancelled")))
    .map_err(|e| internal_error(e, path, "reading response"))?;
    Ok(ok())
}

async fn delete(S3Path { path }: S3Path, state: State) -> Result {
    info!(%path, "deleting");
    let cancel = state.cancel.clone();
    retry(
        async || state.storage.delete(&path, &cancel).await,
        TimeoutOrCancel::caused_by_cancel,
        WARN_THRESHOLD,
        MAX_RETRIES,
        "deleting",
        &cancel,
    )
    .await
    .unwrap_or(Err(anyhow!("deleting cancelled")))
    .map_err(|e| internal_error(e, path, "deleting"))?;
    Ok(ok())
}

async fn delete_prefix(PrefixS3Path { path }: PrefixS3Path, state: State) -> Result {
    info!(%path, "deleting prefix");
    let cancel = state.cancel.clone();
    retry(
        async || state.storage.delete_prefix(&path, &cancel).await,
        TimeoutOrCancel::caused_by_cancel,
        WARN_THRESHOLD,
        MAX_RETRIES,
        "deleting prefix",
        &cancel,
    )
    .await
    .unwrap_or(Err(anyhow!("deleting prefix cancelled")))
    .map_err(|e| internal_error(e, path, "deleting prefix"))?;
    Ok(ok())
}

pub async fn check_storage_permissions(
    client: &GenericRemoteStorage,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    info!("storage permissions check");

    // as_nanos() as multiple instances proxying same bucket may be started at once
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .to_string();

    let path = RemotePath::from_string(&format!("write_access_{now}"))?;
    info!(%path, "uploading");

    let body = now.to_string();
    let stream = bytes_to_stream(Bytes::from(body.clone()));
    client
        .upload(stream, body.len(), &path, None, &cancel)
        .await?;

    use tokio::io::AsyncReadExt;
    info!(%path, "downloading");
    let download_opts = DownloadOpts {
        kind: remote_storage::DownloadKind::Small,
        ..Default::default()
    };
    let mut body_read_buf = Vec::new();
    let stream = client
        .download(&path, &download_opts, &cancel)
        .await?
        .download_stream;
    tokio_util::io::StreamReader::new(stream)
        .read_to_end(&mut body_read_buf)
        .await?;
    let body_read = String::from_utf8(body_read_buf)?;
    if body != body_read {
        error!(%body, %body_read, "File contents do not match");
        anyhow::bail!("Read back file doesn't match original")
    }

    info!(%path, "removing");
    client.delete(&path, &cancel).await
}

fn bytes_to_stream(bytes: Bytes) -> impl futures::Stream<Item = std::io::Result<Bytes>> {
    futures::stream::once(futures::future::ready(Ok(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, extract::Request, response::Response};
    use http_body_util::BodyExt;
    use itertools::iproduct;
    use std::env::var;
    use std::sync::Arc;
    use std::time::Duration;
    use test_log::test as testlog;
    use tower::{Service, util::ServiceExt};
    use utils::id::{TenantId, TimelineId};

    // see libs/remote_storage/tests/test_real_s3.rs
    const REAL_S3_ENV: &str = "ENABLE_REAL_S3_REMOTE_STORAGE";
    const REAL_S3_BUCKET: &str = "REMOTE_STORAGE_S3_BUCKET";
    const REAL_S3_REGION: &str = "REMOTE_STORAGE_S3_REGION";

    async fn proxy() -> (Storage, Option<camino_tempfile::Utf8TempDir>) {
        let cancel = CancellationToken::new();
        let (dir, storage) = if var(REAL_S3_ENV).is_err() {
            // tests execute in parallel and we need a new directory for each of them
            let dir = camino_tempfile::tempdir().unwrap();
            let fs =
                remote_storage::LocalFs::new(dir.path().into(), Duration::from_secs(5)).unwrap();
            (Some(dir), GenericRemoteStorage::LocalFs(fs))
        } else {
            // test_real_s3::create_s3_client is hard to reference, reimplementing here
            let millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            use rand::Rng;
            let random = rand::thread_rng().r#gen::<u32>();

            let s3_config = remote_storage::S3Config {
                bucket_name: var(REAL_S3_BUCKET).unwrap(),
                bucket_region: var(REAL_S3_REGION).unwrap(),
                prefix_in_bucket: Some(format!("test_{millis}_{random:08x}/")),
                endpoint: None,
                concurrency_limit: std::num::NonZeroUsize::new(100).unwrap(),
                max_keys_per_list_response: None,
                upload_storage_class: None,
            };
            let bucket = remote_storage::S3Bucket::new(&s3_config, Duration::from_secs(1))
                .await
                .unwrap();
            (None, GenericRemoteStorage::AwsS3(Arc::new(bucket)))
        };

        let proxy = Storage {
            auth: endpoint_storage::JwtAuth::new(TEST_PUB_KEY_ED25519).unwrap(),
            storage,
            cancel: cancel.clone(),
            max_upload_file_limit: usize::MAX,
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

    #[testlog(tokio::test)]
    async fn status() {
        let res = Request::builder()
            .uri("/status")
            .body(Body::empty())
            .map(request)
            .unwrap()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
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
            let res = Request::builder()
                .uri(uri)
                .method(method)
                .body(Body::empty())
                .map(request)
                .unwrap()
                .await;
            assert!(matches!(
                res.status(),
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

    const TENANT_ID: TenantId =
        TenantId::from_array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6]);
    const TIMELINE_ID: TimelineId =
        TimelineId::from_array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 7]);
    const ENDPOINT_ID: &str = "ep-winter-frost-a662z3vg";
    fn token() -> String {
        let claims = endpoint_storage::claims::EndpointStorageClaims {
            tenant_id: TENANT_ID,
            timeline_id: TIMELINE_ID,
            endpoint_id: ENDPOINT_ID.into(),
            exp: u64::MAX,
        };
        let key = jsonwebtoken::EncodingKey::from_ed_pem(TEST_PRIV_KEY_ED25519).unwrap();
        let header = jsonwebtoken::Header::new(endpoint_storage::VALIDATION_ALGO);
        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    }

    #[testlog(tokio::test)]
    async fn unauthorized() {
        let (proxy, _) = proxy().await;
        let mut app = app(Arc::new(proxy)).into_service();
        let token = token();
        let args = itertools::iproduct!(
            vec![TENANT_ID.to_string(), TenantId::generate().to_string()],
            vec![TIMELINE_ID.to_string(), TimelineId::generate().to_string()],
            vec![ENDPOINT_ID, "ep-ololo"]
        )
        // first one is fully valid path, second path is valid for GET as
        // read paths may have different endpoint if tenant and timeline matches
        // (needed for prewarming RO->RW replica)
        .skip(2);

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
        chain: impl Iterator<Item = (String, &str, &'static str, StatusCode, bool)>,
        token: impl Fn(&str) -> String,
    ) {
        let (proxy, _) = proxy().await;
        let mut app = app(Arc::new(proxy)).into_service();
        for (uri, method, body, expected_status, compare_body) in chain {
            info!(%uri, %method, %body, %expected_status);
            let bearer = format!("Bearer {}", token(&uri));
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
            let read_body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, read_body);
        }
    }

    #[testlog(tokio::test)]
    async fn metrics() {
        let uri = format!("/{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/key");
        let req = vec![
            (uri.clone(), "PUT", "body", StatusCode::OK, false),
            (uri.clone(), "DELETE", "", StatusCode::OK, false),
        ];
        requests_chain(req.into_iter(), |_| token()).await;

        let res = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .map(request)
            .unwrap()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&body);
        tracing::debug!(%body);
        // Storage metrics are not gathered for LocalFs
        if var(REAL_S3_ENV).is_ok() {
            assert!(body.contains("remote_storage_s3_deleted_objects_total"));
        }
        assert!(body.contains("process_threads"));
    }

    #[testlog(tokio::test)]
    async fn insert_retrieve_remove() {
        let uri = format!("/{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/key");
        let chain = vec![
            (uri.clone(), "GET", "", StatusCode::NOT_FOUND, false),
            (uri.clone(), "PUT", "пыщьпыщь", StatusCode::OK, false),
            (uri.clone(), "GET", "пыщьпыщь", StatusCode::OK, true),
            (uri.clone(), "DELETE", "", StatusCode::OK, false),
            (uri, "GET", "", StatusCode::NOT_FOUND, false),
        ];
        requests_chain(chain.into_iter(), |_| token()).await;
    }

    #[testlog(tokio::test)]
    async fn read_other_endpoint_data() {
        let uri = format!("/{TENANT_ID}/{TIMELINE_ID}/other_endpoint/key");
        let chain = vec![
            (uri.clone(), "GET", "", StatusCode::NOT_FOUND, false),
            (uri.clone(), "PUT", "", StatusCode::UNAUTHORIZED, false),
        ];
        requests_chain(chain.into_iter(), |_| token()).await;
    }

    fn delete_prefix_token(uri: &str) -> String {
        let parts = uri.split("/").collect::<Vec<&str>>();
        let claims = endpoint_storage::claims::DeletePrefixClaims {
            tenant_id: parts.get(1).map(|c| c.parse().unwrap()).unwrap(),
            timeline_id: parts.get(2).map(|c| c.parse().unwrap()),
            endpoint_id: parts.get(3).map(ToString::to_string),
            exp: u64::MAX,
        };
        let key = jsonwebtoken::EncodingKey::from_ed_pem(TEST_PRIV_KEY_ED25519).unwrap();
        let header = jsonwebtoken::Header::new(endpoint_storage::VALIDATION_ALGO);
        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    }

    // Can't use single digit numbers as they won't be validated as TimelineId and EndpointId
    #[testlog(tokio::test)]
    async fn delete_prefix() {
        let tenant_id =
            TenantId::from_array([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).to_string();
        let t2 = TimelineId::from_array([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let t3 = TimelineId::from_array([3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let t4 = TimelineId::from_array([4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let f = |timeline, path| format!("/{tenant_id}/{timeline}{path}");
        // Why extra slash in string literals? Axum is weird with URIs:
        // /1/2 and 1/2/ match different routes, thus first yields OK and second NOT_FOUND
        //  as it matches /tenant/timeline/endpoint, see https://stackoverflow.com/a/75355932
        // The cost of removing trailing slash is suprisingly hard:
        // * Add tower dependency with NormalizePath layer
        // * wrap Router<()> in this layer https://github.com/tokio-rs/axum/discussions/2377
        // * Rewrite make_service() -> into_make_service()
        // * Rewrite oneshot() (not available for NormalizePath)
        // I didn't manage to get it working correctly
        let chain = vec![
            // create 1/2/3/4, 1/2/3/5, delete prefix 1/2/3 -> empty
            (f(t2, "/3/4"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "PUT", "", StatusCode::OK, false), // we can override file contents
            (f(t2, "/3/5"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/3"), "DELETE", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t2, "/3/5"), "GET", "", StatusCode::NOT_FOUND, false),
            // create 1/2/3/4, 1/2/5/6, delete prefix 1/2/3 -> 1/2/5/6
            (f(t2, "/3/4"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/5/6"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/3"), "DELETE", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t2, "/5/6"), "GET", "", StatusCode::OK, false),
            // create 1/2/3/4, 1/2/7/8, delete prefix 1/2 -> empty
            (f(t2, "/3/4"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/7/8"), "PUT", "", StatusCode::OK, false),
            (f(t2, ""), "DELETE", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t2, "/7/8"), "GET", "", StatusCode::NOT_FOUND, false),
            // create 1/2/3/4, 1/2/5/6, 1/3/8/9, delete prefix 1/2/3 -> 1/2/5/6, 1/3/8/9
            (f(t2, "/3/4"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/5/6"), "PUT", "", StatusCode::OK, false),
            (f(t3, "/8/9"), "PUT", "", StatusCode::OK, false),
            (f(t2, "/3"), "DELETE", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t2, "/5/6"), "GET", "", StatusCode::OK, false),
            (f(t3, "/8/9"), "GET", "", StatusCode::OK, false),
            // create 1/4/5/6, delete prefix 1/2 -> 1/3/8/9, 1/4/5/6
            (f(t4, "/5/6"), "PUT", "", StatusCode::OK, false),
            (f(t2, ""), "DELETE", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t2, "/5/6"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t3, "/8/9"), "GET", "", StatusCode::OK, false),
            (f(t4, "/5/6"), "GET", "", StatusCode::OK, false),
            // delete prefix 1 -> empty
            (format!("/{tenant_id}"), "DELETE", "", StatusCode::OK, false),
            (f(t2, "/3/4"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t2, "/5/6"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t3, "/8/9"), "GET", "", StatusCode::NOT_FOUND, false),
            (f(t4, "/5/6"), "GET", "", StatusCode::NOT_FOUND, false),
        ];
        requests_chain(chain.into_iter(), delete_prefix_token).await;
    }
}
