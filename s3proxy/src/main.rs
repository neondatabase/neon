use anyhow::{Context, Result, bail};
use axum::body::{Body, BodyDataStream};
use axum::extract::State as AxumState;
use axum::response::{IntoResponse, Response};
use axum::{extract::Path, http::StatusCode, routing::get};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use jsonwebtoken::{DecodingKey, Validation};
use remote_storage::{DownloadError, DownloadOpts, GenericRemoteStorage, RemotePath};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const HELP: &str = "s3 proxy:
If \"type\" is \"aws\", cli may look up the following environment variables:
 AWS_ENDPOINT_URL
 AWS_ACCESS_KEY_ID
 AWS_SECRET_ACCESS_KEY
 or others, see https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html
In case of \"azure\", it may look up the following:
 AZURE_STORAGE_ACCOUNT
 AZURE_STORAGE_ACCESS_KEY";

#[derive(clap::ValueEnum, Clone)]
#[clap(rename_all = "kebab_case")]
enum StorageType {
    AWS,
    Azure,
}

#[derive(Parser)]
#[command(version, about, long_about = HELP)]
struct Args {
    #[arg(short, long)]
    listen: std::net::SocketAddr,
    #[arg(short, long)]
    bucket: String,
    #[arg(short, long)]
    region: String,
    #[arg(short = 't', long = "type")]
    storage_type: StorageType,
    #[arg(short, long, help = "Public key for verifying JWT tokens")]
    pemfile: Utf8PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    utils::logging::init(
        utils::logging::LogFormat::Plain,
        utils::logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
        utils::logging::Output::Stdout,
    )?;
    let args = Args::parse();
    let listener = tokio::net::TcpListener::bind(args.listen).await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());
    let pemfile = std::fs::read(args.pemfile).context("reading pemfile")?;
    let auth = JwtAuth::new(&pemfile).context("loading JwtAuth")?;

    let concurrency_limit = std::num::NonZero::new(10).unwrap(); // TODO?
    let timeout = Duration::from_secs(3);
    let azure_small_timeout = Duration::from_secs(1);
    let azure_conn_pool_size = 10;
    let storage = if let StorageType::AWS = args.storage_type {
        let config = remote_storage::S3Config {
            bucket_name: args.bucket,
            bucket_region: args.region,
            prefix_in_bucket: None,
            endpoint: None,
            concurrency_limit,
            max_keys_per_list_response: None,
            upload_storage_class: None,
        };
        remote_storage::S3Bucket::new(&config, timeout)
            .await
            .map(Arc::new)
            .map(GenericRemoteStorage::AwsS3)?
    } else {
        let config = remote_storage::AzureConfig {
            container_name: args.bucket,
            storage_account: None,
            container_region: args.region,
            prefix_in_container: None,
            concurrency_limit,
            max_keys_per_list_response: None,
            conn_pool_size: azure_conn_pool_size,
        };
        remote_storage::AzureBlobStorage::new(&config, timeout, azure_small_timeout)
            .map(Arc::new)
            .map(GenericRemoteStorage::AzureBlob)?
    };
    let cancel = CancellationToken::new();
    check_storage_permissions(&storage, cancel.clone()).await?;

    let proxy = Arc::new(Proxy {
        auth,
        storage,
        cancel: cancel.clone(),
    });

    let ctrl_c_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Shutting down");
        ctrl_c_cancel.cancel()
    });
    axum::serve(listener, app().with_state(proxy))
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}

// simplified version of utils::auth::JwtAuth
pub struct JwtAuth {
    decoding_key: DecodingKey,
    validation: Validation,
}

const VALIDATION_ALGO: jsonwebtoken::Algorithm = jsonwebtoken::Algorithm::EdDSA;
impl JwtAuth {
    pub fn new(key: &[u8]) -> Result<Self> {
        let decoding_key = DecodingKey::from_ed_pem(key)?;
        let mut validation = Validation::new(VALIDATION_ALGO);
        validation.required_spec_claims = [].into();
        Ok(Self {
            decoding_key,
            validation,
        })
    }

    pub fn decode(&self, token: &str) -> Result<Claims> {
        Ok(jsonwebtoken::decode(token, &self.decoding_key, &self.validation).map(|t| t.claims)?)
    }
}

async fn check_storage_permissions(
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
    let body_str = format!("{now}");
    let stream = Body::from(body_str.clone()).into_data_stream();
    let stream = body_stream_to_sync_stream(stream);
    client
        .upload(stream, body_str.len(), &path, None, &cancel)
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
    let body_read_str = String::from_utf8(body_read_buf)?;
    if body_str != body_read_str {
        bail!("{} (original) != {} (read back)", body_str, body_read_str)
    }

    debug!(%path, "removing");
    client.delete(&path, &cancel).await
}

fn body_stream_to_sync_stream(
    stream: BodyDataStream,
) -> impl futures::Stream<Item = std::io::Result<axum::body::Bytes>> {
    use futures::stream::TryStreamExt;
    sync_wrapper::SyncStream::new(stream)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

fn app() -> Router {
    let router = Router::new().route(
        "/{tenant_id}/{timeline_id}/{endpoint_id}/{*path}",
        get(get_key).put(set_key).delete(delete_key),
    );
    // axum-prometheus doesn't work with tokio tests
    // "Failed to set global recorder: SetRecorderError"
    if cfg!(test) {
        return router;
    }
    // TODO add remote_storage/metrics/bucket_metrics?
    let (layer, handle) = axum_prometheus::PrometheusMetricLayer::pair();
    router
        .layer(layer)
        .route("/metrics", get(|| async move { handle.render() }))
}

async fn get_key(path: KeyPath, state: AxumState<State>) -> impl IntoResponse {
    merge(get_key_impl(path, state).await)
}

fn merge(res: Result<Response, Response>) -> Response {
    match res {
        Ok(v) => v,
        Err(e) => e,
    }
}

fn unauthorized() -> Response {
    StatusCode::UNAUTHORIZED.into_response()
}
fn bad_request(e: impl ToString) -> Response {
    (StatusCode::BAD_REQUEST, e.to_string()).into_response()
}
fn internal_error() -> Response {
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}
fn not_found(key: impl ToString) -> Response {
    (StatusCode::NOT_FOUND, key.to_string()).into_response()
}

const CONTENT_TYPE: &str = "content-type";
const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";
async fn get_key_impl(key_path: KeyPath, state: AxumState<State>) -> Result<Response, Response> {
    let path = construct_s3_key(&key_path).map_err(bad_request)?;
    let opts = DownloadOpts::default();
    info!(path = path.to_string(), "Downloading");

    let stream = state
        .storage
        .download(&path, &opts, &state.cancel.clone())
        .await
        .map_err(|err| {
            error!(
                %path,
                %err,
                "Error downloading"
            );
            match err {
                DownloadError::NotFound => not_found(path.to_string()),
                _ => internal_error(),
            }
        })?
        .download_stream;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        .body(Body::from_stream(stream))
        .map_err(|_| internal_error())
}

fn construct_s3_key(key_path: &KeyPath) -> Result<RemotePath> {
    let auth = &key_path.auth;
    let buf = format!(
        "{}/{}/{}",
        auth.tenant_id, auth.timeline_id, auth.endpoint_id
    );
    RemotePath::new(&Utf8PathBuf::from(buf).join(normalize_key(&key_path.path)?))
}

fn normalize_key(key: &str) -> Result<Utf8PathBuf> {
    let key = clean_utf8(&Utf8PathBuf::from(key));
    if key.starts_with("..") || key == Utf8PathBuf::from(".") || key == Utf8PathBuf::from("/") {
        bail!("Invalid key {key}")
    }
    let Ok(path) = key.strip_prefix("/").map(Utf8PathBuf::from) else {
        return Ok(key);
    };
    Ok(path)
}

// Copied from path_clean crate with PathBuf->Utf8PathBuf
fn clean_utf8(path: &Utf8Path) -> Utf8PathBuf {
    use camino::Utf8Component;
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            Utf8Component::CurDir => (),
            Utf8Component::ParentDir => match out.last() {
                Some(Utf8Component::RootDir) => (),
                Some(Utf8Component::Normal(_)) => {
                    out.pop();
                }
                None
                | Some(Utf8Component::CurDir)
                | Some(Utf8Component::ParentDir)
                | Some(Utf8Component::Prefix(_)) => out.push(comp),
            },
            comp => out.push(comp),
        }
    }
    if !out.is_empty() {
        out.iter().collect()
    } else {
        Utf8PathBuf::from(".")
    }
}

async fn set_key(path: KeyPath, state: AxumState<State>, body: Body) -> impl IntoResponse {
    merge(set_key_impl(path, state, body).await)
}

async fn set_key_impl(
    key_path: KeyPath,
    state: AxumState<State>,
    body: Body,
) -> Result<Response, Response> {
    let remote_path = construct_s3_key(&key_path).map_err(bad_request)?;
    info!(path = remote_path.to_string(), "Uploading");

    use axum::body::HttpBody;
    let stream = body.into_data_stream();
    // TODO Get stream hint size without reading it into Bytes
    let body_size = stream.size_hint().upper().unwrap_or(0) as usize;
    let stream = body_stream_to_sync_stream(stream);
    state
        .storage
        .upload(stream, body_size, &remote_path, None, &state.cancel.clone())
        .await
        .map(|_| StatusCode::OK.into_response())
        .map_err(|err| {
            error!(
                %remote_path,
                %err,
                "Error uploading"
            );
            internal_error()
        })
}

async fn delete_key(path: KeyPath, state: AxumState<State>) -> impl IntoResponse {
    merge(delete_key_impl(path, state).await)
}

async fn delete_key_impl(key_path: KeyPath, state: AxumState<State>) -> Result<Response, Response> {
    let path = construct_s3_key(&key_path).map_err(bad_request)?;
    debug!(%path, "Deleting");
    state
        .storage
        .delete(&path, &state.cancel.clone())
        .await
        .map(|_| StatusCode::OK.into_response())
        .map_err(|err| {
            error!(
                %path,
                %err,
                "Error deleting"
            );
            internal_error()
        })
}

struct Proxy {
    auth: JwtAuth,
    storage: GenericRemoteStorage,
    cancel: CancellationToken,
}
type State = Arc<Proxy>;
type Router = axum::Router<State>;

// libs/utils/src/id.rs has TenantId and TimelineId but we don't need
// them as both types are used as strings.
// Validity is checked via JWT verification so we're fine to ignore parsing rules
// If needed further, we can use smallstr::SmallString<[u8; 16]>;
type TimelineId = String;
type TenantId = String;
type EndpointId = String;

#[derive(Deserialize, Serialize, PartialEq)]
pub struct Claims {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    endpoint_id: EndpointId,
}

#[derive(Deserialize, Serialize)]
struct KeyPath {
    #[serde(flatten)]
    auth: Claims,
    path: String,
}

use axum::RequestPartsExt;
use axum_extra::TypedHeader;
use axum_extra::headers::{Authorization, authorization::Bearer};
impl axum::extract::FromRequestParts<State> for KeyPath {
    type Rejection = Response;
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &State,
    ) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| bad_request("invalid token"))?;
        let claims = state
            .auth
            .decode(bearer.token())
            .map_err(|_| bad_request("invalid token"))?;
        let Path(path) = parts
            .extract::<Path<KeyPath>>()
            .await
            .map_err(|_| bad_request("invalid route"))?;
        if path.auth != claims {
            return Err(unauthorized());
        }
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, extract::Request, response::Response};
    use parameterized::parameterized;
    use tempfile::TempDir;
    use test_log::test as testlog;
    use tower::{Service, util::ServiceExt};

    async fn proxy() -> (Proxy, TempDir) {
        // tests execute in parallel and we need a new directory for each of them
        let dir = tempfile::tempdir().unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
        let fs = remote_storage::LocalFs::new(path, Duration::from_secs(5)).unwrap();
        let cancel = CancellationToken::new();
        let proxy = Proxy {
            auth: JwtAuth::new(TEST_PUB_KEY_ED25519).unwrap(),
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
        app()
            .with_state(Arc::new(proxy))
            .into_service()
            .oneshot(req)
            .await
            .unwrap()
    }

    #[parameterized(method = { "GET", "PUT", "DELETE" })]
    #[parameterized_macro(testlog(tokio::test))]
    async fn no_token(method: &str) {
        let status = Request::builder()
            .uri("/1/2/3/4")
            .method(method)
            .body(Body::empty())
            .map(request)
            .unwrap()
            .await
            .status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[parameterized(method = { "GET", "PUT", "DELETE" })]
    #[parameterized_macro(testlog(tokio::test))]
    async fn invalid_token(method: &str) {
        let status = Request::builder()
            .uri("/1/2/3/4")
            .header("Authorization", "Bearer 123")
            .method(method)
            .body(Body::empty())
            .map(request)
            .unwrap()
            .await
            .status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    const TENANT_ID: &str = "1adcba3c01c578d1c1be7b8048a4484d";
    const TIMELINE_ID: &str = "16fde223c5e55c4b791e63284681d951";
    const ENDPOINT_ID: &str = "ep-winter-frost-a662z3vg";
    fn token() -> String {
        let claims = Claims {
            tenant_id: TENANT_ID.into(),
            timeline_id: TIMELINE_ID.into(),
            endpoint_id: ENDPOINT_ID.into(),
        };
        let key = jsonwebtoken::EncodingKey::from_ed_pem(TEST_PRIV_KEY_ED25519).unwrap();
        jsonwebtoken::encode(&jsonwebtoken::Header::new(VALIDATION_ALGO), &claims, &key).unwrap()
    }

    #[parameterized(method = { "GET", "PUT", "DELETE" })]
    #[parameterized_macro(testlog(tokio::test))]
    async fn unauthorized(method: &str) {
        let (proxy, _) = proxy().await;
        let mut app = app().with_state(Arc::new(proxy)).into_service();
        let token = token();
        let triples = itertools::iproduct!(
            vec![TENANT_ID, "12345"],
            vec![TIMELINE_ID, "12345"],
            vec![ENDPOINT_ID, "ep-ololo"]
        );
        for (tenant, timeline, endpoint) in triples.skip(1) {
            let request = Request::builder()
                .uri(format!("/{tenant}/{timeline}/{endpoint}/sub/path/key"))
                .method(method)
                .header("Authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap();
            let status = ServiceExt::<Request<Body>>::ready(&mut app)
                .await
                .unwrap()
                .call(request)
                .await
                .unwrap()
                .status();
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        }
    }

    #[parameterized(method = { "GET", "PUT", "DELETE" })]
    #[parameterized_macro(testlog(tokio::test))]
    async fn invalid_cache_key(method: &str) {
        let token = token();
        let status = Request::builder()
            .uri(format!("/{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/.."))
            .method(method)
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .map(request)
            .unwrap()
            .await
            .status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[testlog(tokio::test)]
    async fn get_put_get_delete_get() {
        let (proxy, _) = proxy().await;
        let mut app = app().with_state(Arc::new(proxy)).into_service();
        let bearer = format!("Bearer {}", token());
        let uri = format!("/{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/key");

        let request = Request::builder()
            .uri(&uri)
            .header("Authorization", &bearer)
            .body(Body::empty())
            .unwrap();
        let status = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::NOT_FOUND);

        let content = "пыщьпыщь";
        let request = Request::builder()
            .uri(&uri)
            .header("Authorization", &bearer)
            .method("PUT")
            .body(Body::from(content))
            .unwrap();
        let status = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::OK);

        let request = Request::builder()
            .uri(&uri)
            .header("Authorization", &bearer)
            .body(Body::empty())
            .unwrap();
        let response = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        use http_body_util::BodyExt;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, content);

        let request = Request::builder()
            .uri(&uri)
            .header("Authorization", &bearer)
            .method("DELETE")
            .body(Body::empty())
            .unwrap();
        let status = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::OK);

        let request = Request::builder()
            .uri(&uri)
            .header("Authorization", &bearer)
            .body(Body::empty())
            .unwrap();
        let status = ServiceExt::<Request<Body>>::ready(&mut app)
            .await
            .unwrap()
            .call(request)
            .await
            .unwrap()
            .status();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn normalize_key() {
        let f = super::normalize_key;
        assert_eq!(f("hello/world/..").unwrap(), Utf8PathBuf::from("hello"));
        assert_eq!(
            f("ololo/1/../../not_ololo").unwrap(),
            Utf8PathBuf::from("not_ololo")
        );
        assert!(f("ololo/1/../../../").is_err());
        assert!(f(".").is_err());
        assert!(f("../").is_err());
        assert!(f("").is_err());
        assert_eq!(f("/1/2/3").unwrap(), Utf8PathBuf::from("1/2/3"));
        assert!(f("/1/2/3/../../../").is_err());
        assert!(f("/1/2/3/../../../../").is_err());
    }

    #[test]
    fn construct_s3_key() {
        let f = super::construct_s3_key;
        let auth = Claims {
            tenant_id: TENANT_ID.into(),
            timeline_id: TIMELINE_ID.into(),
            endpoint_id: ENDPOINT_ID.into(),
        };
        let remote_path = |key| {
            RemotePath::from_string(&format!("{TENANT_ID}/{TIMELINE_ID}/{ENDPOINT_ID}/{key}"))
                .unwrap()
        };

        let mut key_path = KeyPath {
            auth,
            path: "cache_key".to_string(),
        };
        assert_eq!(f(&key_path).unwrap(), remote_path(key_path.path));

        key_path.path = "we/can/have/nested/paths".to_string();
        assert_eq!(f(&key_path).unwrap(), remote_path(key_path.path));

        key_path.path = "../error/hello/../".to_string();
        assert!(f(&key_path).is_err());
    }
}
