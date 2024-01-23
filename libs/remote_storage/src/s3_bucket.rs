//! AWS S3 storage wrapper around `rusoto` library.
//!
//! Respects `prefix_in_bucket` property from [`S3Config`],
//! allowing multiple api users to independently work with the same S3 bucket, if
//! their bucket prefixes are both specified and different.

use std::{
    borrow::Cow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Context as _;
use aws_config::{
    environment::credentials::EnvironmentVariableCredentialsProvider,
    imds::credentials::ImdsCredentialsProvider,
    meta::credentials::CredentialsProviderChain,
    profile::ProfileFileCredentialsProvider,
    provider_config::ProviderConfig,
    retry::{RetryConfigBuilder, RetryMode},
    web_identity_token::WebIdentityTokenCredentialsProvider,
    BehaviorVersion,
};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{
    config::{AsyncSleep, Builder, IdentityCache, Region, SharedAsyncSleep},
    error::SdkError,
    operation::get_object::GetObjectError,
    types::{Delete, ObjectIdentifier},
    Client,
};
use aws_smithy_async::rt::sleep::TokioSleep;

use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use futures::stream::Stream;
use futures_util::TryStreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use scopeguard::ScopeGuard;

use super::StorageMetadata;
use crate::{
    ConcurrencyLimiter, Download, DownloadError, Listing, ListingMode, RemotePath, RemoteStorage,
    S3Config, MAX_KEYS_PER_DELETE, REMOTE_STORAGE_PREFIX_SEPARATOR,
};

pub(super) mod metrics;

use self::metrics::AttemptOutcome;
pub(super) use self::metrics::RequestKind;

/// AWS S3 storage.
pub struct S3Bucket {
    client: Client,
    bucket_name: String,
    prefix_in_bucket: Option<String>,
    max_keys_per_list_response: Option<i32>,
    concurrency_limiter: ConcurrencyLimiter,
}

#[derive(Default)]
struct GetObjectRequest {
    bucket: String,
    key: String,
    range: Option<String>,
}
impl S3Bucket {
    /// Creates the S3 storage, errors if incorrect AWS S3 configuration provided.
    pub fn new(aws_config: &S3Config) -> anyhow::Result<Self> {
        tracing::debug!(
            "Creating s3 remote storage for S3 bucket {}",
            aws_config.bucket_name
        );

        let region = Some(Region::new(aws_config.bucket_region.clone()));

        let provider_conf = ProviderConfig::without_region().with_region(region.clone());

        let credentials_provider = {
            // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
            CredentialsProviderChain::first_try(
                "env",
                EnvironmentVariableCredentialsProvider::new(),
            )
            // uses "AWS_PROFILE" / `aws sso login --profile <profile>`
            .or_else(
                "profile-sso",
                ProfileFileCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build(),
            )
            // uses "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME"
            // needed to access remote extensions bucket
            .or_else(
                "token",
                WebIdentityTokenCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build(),
            )
            // uses imds v2
            .or_else("imds", ImdsCredentialsProvider::builder().build())
        };

        // AWS SDK requires us to specify how the RetryConfig should sleep when it wants to back off
        let sleep_impl: Arc<dyn AsyncSleep> = Arc::new(TokioSleep::new());

        // We do our own retries (see [`backoff::retry`]).  However, for the AWS SDK to enable rate limiting in response to throttling
        // responses (e.g. 429 on too many ListObjectsv2 requests), we must provide a retry config.  We set it to use at most one
        // attempt, and enable 'Adaptive' mode, which causes rate limiting to be enabled.
        let mut retry_config = RetryConfigBuilder::new();
        retry_config
            .set_max_attempts(Some(1))
            .set_mode(Some(RetryMode::Adaptive));

        let mut config_builder = Builder::default()
            .behavior_version(BehaviorVersion::v2023_11_09())
            .region(region)
            .identity_cache(IdentityCache::lazy().build())
            .credentials_provider(SharedCredentialsProvider::new(credentials_provider))
            .retry_config(retry_config.build())
            .sleep_impl(SharedAsyncSleep::from(sleep_impl));

        if let Some(custom_endpoint) = aws_config.endpoint.clone() {
            config_builder = config_builder
                .endpoint_url(custom_endpoint)
                .force_path_style(true);
        }

        let client = Client::from_conf(config_builder.build());

        let prefix_in_bucket = aws_config.prefix_in_bucket.as_deref().map(|prefix| {
            let mut prefix = prefix;
            while prefix.starts_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                prefix = &prefix[1..]
            }

            let mut prefix = prefix.to_string();
            while prefix.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                prefix.pop();
            }
            prefix
        });
        Ok(Self {
            client,
            bucket_name: aws_config.bucket_name.clone(),
            max_keys_per_list_response: aws_config.max_keys_per_list_response,
            prefix_in_bucket,
            concurrency_limiter: ConcurrencyLimiter::new(aws_config.concurrency_limit.get()),
        })
    }

    fn s3_object_to_relative_path(&self, key: &str) -> RemotePath {
        let relative_path =
            match key.strip_prefix(self.prefix_in_bucket.as_deref().unwrap_or_default()) {
                Some(stripped) => stripped,
                // we rely on AWS to return properly prefixed paths
                // for requests with a certain prefix
                None => panic!(
                    "Key {} does not start with bucket prefix {:?}",
                    key, self.prefix_in_bucket
                ),
            };
        RemotePath(
            relative_path
                .split(REMOTE_STORAGE_PREFIX_SEPARATOR)
                .collect(),
        )
    }

    pub fn relative_path_to_s3_object(&self, path: &RemotePath) -> String {
        assert_eq!(std::path::MAIN_SEPARATOR, REMOTE_STORAGE_PREFIX_SEPARATOR);
        let path_string = path
            .get_path()
            .as_str()
            .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR);
        match &self.prefix_in_bucket {
            Some(prefix) => prefix.clone() + "/" + path_string,
            None => path_string.to_string(),
        }
    }

    async fn permit(&self, kind: RequestKind) -> tokio::sync::SemaphorePermit<'_> {
        let started_at = start_counting_cancelled_wait(kind);
        let permit = self
            .concurrency_limiter
            .acquire(kind)
            .await
            .expect("semaphore is never closed");

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);

        permit
    }

    async fn owned_permit(&self, kind: RequestKind) -> tokio::sync::OwnedSemaphorePermit {
        let started_at = start_counting_cancelled_wait(kind);
        let permit = self
            .concurrency_limiter
            .acquire_owned(kind)
            .await
            .expect("semaphore is never closed");

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);
        permit
    }

    async fn download_object(&self, request: GetObjectRequest) -> Result<Download, DownloadError> {
        let kind = RequestKind::Get;
        let permit = self.owned_permit(kind).await;

        let started_at = start_measuring_requests(kind);

        let get_object = self
            .client
            .get_object()
            .bucket(request.bucket)
            .key(request.key)
            .set_range(request.range)
            .send()
            .await;

        let started_at = ScopeGuard::into_inner(started_at);

        match get_object {
            Ok(object_output) => {
                let metadata = object_output.metadata().cloned().map(StorageMetadata);
                let etag = object_output.e_tag.clone();
                let last_modified = object_output.last_modified.and_then(|t| t.try_into().ok());

                let body = object_output.body;
                let body = ByteStreamAsStream::from(body);
                let body = PermitCarrying::new(permit, body);
                let body = TimedDownload::new(started_at, body);

                Ok(Download {
                    metadata,
                    etag,
                    last_modified,
                    download_stream: Box::pin(body),
                })
            }
            Err(SdkError::ServiceError(e)) if matches!(e.err(), GetObjectError::NoSuchKey(_)) => {
                // Count this in the AttemptOutcome::Ok bucket, because 404 is not
                // an error: we expect to sometimes fetch an object and find it missing,
                // e.g. when probing for timeline indices.
                metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Ok,
                    started_at,
                );
                Err(DownloadError::NotFound)
            }
            Err(e) => {
                metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Err,
                    started_at,
                );

                Err(DownloadError::Other(
                    anyhow::Error::new(e).context("download s3 object"),
                ))
            }
        }
    }
}

pin_project_lite::pin_project! {
    struct ByteStreamAsStream {
        #[pin]
        inner: aws_smithy_types::byte_stream::ByteStream
    }
}

impl From<aws_smithy_types::byte_stream::ByteStream> for ByteStreamAsStream {
    fn from(inner: aws_smithy_types::byte_stream::ByteStream) -> Self {
        ByteStreamAsStream { inner }
    }
}

impl Stream for ByteStreamAsStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // this does the std::io::ErrorKind::Other conversion
        self.project().inner.poll_next(cx).map_err(|x| x.into())
    }

    // cannot implement size_hint because inner.size_hint is remaining size in bytes, which makes
    // sense and Stream::size_hint does not really
}

pin_project_lite::pin_project! {
    /// An `AsyncRead` adapter which carries a permit for the lifetime of the value.
    struct PermitCarrying<S> {
        permit: tokio::sync::OwnedSemaphorePermit,
        #[pin]
        inner: S,
    }
}

impl<S> PermitCarrying<S> {
    fn new(permit: tokio::sync::OwnedSemaphorePermit, inner: S) -> Self {
        Self { permit, inner }
    }
}

impl<S: Stream<Item = std::io::Result<Bytes>>> Stream for PermitCarrying<S> {
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pin_project_lite::pin_project! {
    /// Times and tracks the outcome of the request.
    struct TimedDownload<S> {
        started_at: std::time::Instant,
        outcome: metrics::AttemptOutcome,
        #[pin]
        inner: S
    }

    impl<S> PinnedDrop for TimedDownload<S> {
        fn drop(mut this: Pin<&mut Self>) {
            metrics::BUCKET_METRICS.req_seconds.observe_elapsed(RequestKind::Get, this.outcome, this.started_at);
        }
    }
}

impl<S> TimedDownload<S> {
    fn new(started_at: std::time::Instant, inner: S) -> Self {
        TimedDownload {
            started_at,
            outcome: metrics::AttemptOutcome::Cancelled,
            inner,
        }
    }
}

impl<S: Stream<Item = std::io::Result<Bytes>>> Stream for TimedDownload<S> {
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::task::ready;

        let this = self.project();

        let res = ready!(this.inner.poll_next(cx));
        match &res {
            Some(Ok(_)) => {}
            Some(Err(_)) => *this.outcome = metrics::AttemptOutcome::Err,
            None => *this.outcome = metrics::AttemptOutcome::Ok,
        }

        Poll::Ready(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[async_trait::async_trait]
impl RemoteStorage for S3Bucket {
    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
    ) -> Result<Listing, DownloadError> {
        let kind = RequestKind::List;
        let mut result = Listing::default();

        // get the passed prefix or if it is not set use prefix_in_bucket value
        let list_prefix = prefix
            .map(|p| self.relative_path_to_s3_object(p))
            .or_else(|| self.prefix_in_bucket.clone())
            .map(|mut p| {
                // required to end with a separator
                // otherwise request will return only the entry of a prefix
                if matches!(mode, ListingMode::WithDelimiter)
                    && !p.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR)
                {
                    p.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                }
                p
            });

        let mut continuation_token = None;

        loop {
            let _guard = self.permit(kind).await;
            let started_at = start_measuring_requests(kind);

            let mut request = self
                .client
                .list_objects_v2()
                .bucket(self.bucket_name.clone())
                .set_prefix(list_prefix.clone())
                .set_continuation_token(continuation_token)
                .set_max_keys(self.max_keys_per_list_response);

            if let ListingMode::WithDelimiter = mode {
                request = request.delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string());
            }

            let response = request
                .send()
                .await
                .context("Failed to list S3 prefixes")
                .map_err(DownloadError::Other);

            let started_at = ScopeGuard::into_inner(started_at);

            metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &response, started_at);

            let response = response?;

            let keys = response.contents();
            let empty = Vec::new();
            let prefixes = response.common_prefixes.as_ref().unwrap_or(&empty);

            tracing::debug!("list: {} prefixes, {} keys", prefixes.len(), keys.len());

            for object in keys {
                let object_path = object.key().expect("response does not contain a key");
                let remote_path = self.s3_object_to_relative_path(object_path);
                result.keys.push(remote_path);
            }

            result.prefixes.extend(
                prefixes
                    .iter()
                    .filter_map(|o| Some(self.s3_object_to_relative_path(o.prefix()?))),
            );

            continuation_token = match response.next_continuation_token {
                Some(new_token) => Some(new_token),
                None => break,
            };
        }

        Ok(result)
    }

    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Put;
        let _guard = self.permit(kind).await;

        let started_at = start_measuring_requests(kind);

        let body = StreamBody::new(from.map_ok(Frame::data));
        let bytes_stream = ByteStream::new(SdkBody::from_body_1_x(body));

        let res = self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(to))
            .set_metadata(metadata.map(|m| m.0))
            .content_length(from_size_bytes.try_into()?)
            .body(bytes_stream)
            .send()
            .await;

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        res?;

        Ok(())
    }

    async fn copy(&self, from: &RemotePath, to: &RemotePath) -> anyhow::Result<()> {
        let kind = RequestKind::Copy;
        let _guard = self.permit(kind).await;

        let started_at = start_measuring_requests(kind);

        // we need to specify bucket_name as a prefix
        let copy_source = format!(
            "{}/{}",
            self.bucket_name,
            self.relative_path_to_s3_object(from)
        );

        let res = self
            .client
            .copy_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(to))
            .copy_source(copy_source)
            .send()
            .await;

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        res?;

        Ok(())
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        // if prefix is not none then download file `prefix/from`
        // if prefix is none then download file `from`
        self.download_object(GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.relative_path_to_s3_object(from),
            range: None,
        })
        .await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        // S3 accepts ranges as https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        // and needs both ends to be exclusive
        let end_inclusive = end_exclusive.map(|end| end.saturating_sub(1));
        let range = Some(match end_inclusive {
            Some(end_inclusive) => format!("bytes={start_inclusive}-{end_inclusive}"),
            None => format!("bytes={start_inclusive}-"),
        });

        self.download_object(GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.relative_path_to_s3_object(from),
            range,
        })
        .await
    }
    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let _guard = self.permit(kind).await;

        let mut delete_objects = Vec::with_capacity(paths.len());
        for path in paths {
            let obj_id = ObjectIdentifier::builder()
                .set_key(Some(self.relative_path_to_s3_object(path)))
                .build()?;
            delete_objects.push(obj_id);
        }

        for chunk in delete_objects.chunks(MAX_KEYS_PER_DELETE) {
            let started_at = start_measuring_requests(kind);

            let resp = self
                .client
                .delete_objects()
                .bucket(self.bucket_name.clone())
                .delete(
                    Delete::builder()
                        .set_objects(Some(chunk.to_vec()))
                        .build()?,
                )
                .send()
                .await;

            let started_at = ScopeGuard::into_inner(started_at);
            metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &resp, started_at);

            match resp {
                Ok(resp) => {
                    metrics::BUCKET_METRICS
                        .deleted_objects_total
                        .inc_by(chunk.len() as u64);
                    if let Some(errors) = resp.errors {
                        // Log a bounded number of the errors within the response:
                        // these requests can carry 1000 keys so logging each one
                        // would be too verbose, especially as errors may lead us
                        // to retry repeatedly.
                        const LOG_UP_TO_N_ERRORS: usize = 10;
                        for e in errors.iter().take(LOG_UP_TO_N_ERRORS) {
                            tracing::warn!(
                                "DeleteObjects key {} failed: {}: {}",
                                e.key.as_ref().map(Cow::from).unwrap_or("".into()),
                                e.code.as_ref().map(Cow::from).unwrap_or("".into()),
                                e.message.as_ref().map(Cow::from).unwrap_or("".into())
                            );
                        }

                        return Err(anyhow::format_err!(
                            "Failed to delete {} objects",
                            errors.len()
                        ));
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        let paths = std::array::from_ref(path);
        self.delete_objects(paths).await
    }
}

/// On drop (cancellation) count towards [`metrics::BucketMetrics::cancelled_waits`].
fn start_counting_cancelled_wait(
    kind: RequestKind,
) -> ScopeGuard<std::time::Instant, impl FnOnce(std::time::Instant), scopeguard::OnSuccess> {
    scopeguard::guard_on_success(std::time::Instant::now(), move |_| {
        metrics::BUCKET_METRICS.cancelled_waits.get(kind).inc()
    })
}

/// On drop (cancellation) add time to [`metrics::BucketMetrics::req_seconds`].
fn start_measuring_requests(
    kind: RequestKind,
) -> ScopeGuard<std::time::Instant, impl FnOnce(std::time::Instant), scopeguard::OnSuccess> {
    scopeguard::guard_on_success(std::time::Instant::now(), move |started_at| {
        metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
            kind,
            AttemptOutcome::Cancelled,
            started_at,
        )
    })
}

#[cfg(test)]
mod tests {
    use camino::Utf8Path;
    use std::num::NonZeroUsize;

    use crate::{RemotePath, S3Bucket, S3Config};

    #[test]
    fn relative_path() {
        let all_paths = ["", "some/path", "some/path/"];
        let all_paths: Vec<RemotePath> = all_paths
            .iter()
            .map(|x| RemotePath::new(Utf8Path::new(x)).expect("bad path"))
            .collect();
        let prefixes = [
            None,
            Some(""),
            Some("test/prefix"),
            Some("test/prefix/"),
            Some("/test/prefix/"),
        ];
        let expected_outputs = vec![
            vec!["", "some/path", "some/path"],
            vec!["/", "/some/path", "/some/path"],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path",
            ],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path",
            ],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path",
            ],
        ];

        for (prefix_idx, prefix) in prefixes.iter().enumerate() {
            let config = S3Config {
                bucket_name: "bucket".to_owned(),
                bucket_region: "region".to_owned(),
                prefix_in_bucket: prefix.map(str::to_string),
                endpoint: None,
                concurrency_limit: NonZeroUsize::new(100).unwrap(),
                max_keys_per_list_response: Some(5),
            };
            let storage = S3Bucket::new(&config).expect("remote storage init");
            for (test_path_idx, test_path) in all_paths.iter().enumerate() {
                let result = storage.relative_path_to_s3_object(test_path);
                let expected = expected_outputs[prefix_idx][test_path_idx];
                assert_eq!(result, expected);
            }
        }
    }
}
