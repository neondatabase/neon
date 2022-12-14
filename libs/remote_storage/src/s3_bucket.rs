//! AWS S3 storage wrapper around `rusoto` library.
//!
//! Respects `prefix_in_bucket` property from [`S3Config`],
//! allowing multiple api users to independently work with the same S3 bucket, if
//! their bucket prefixes are both specified and different.

use std::env::var;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_config::{
    environment::credentials::EnvironmentVariableCredentialsProvider, imds,
    imds::credentials::ImdsCredentialsProvider, meta::credentials::provide_credentials_fn,
};
use aws_sdk_s3::{
    config::Config,
    error::{GetObjectError, GetObjectErrorKind},
    types::{ByteStream, SdkError},
    Client, Endpoint, Region,
};
use aws_smithy_http::body::SdkBody;
use aws_types::credentials::{CredentialsError, ProvideCredentials};
use hyper::Body;
use tokio::{io, sync::Semaphore};
use tokio_util::io::ReaderStream;
use tracing::debug;

use super::StorageMetadata;
use crate::{
    Download, DownloadError, RemotePath, RemoteStorage, S3Config, REMOTE_STORAGE_PREFIX_SEPARATOR,
};

const DEFAULT_IMDS_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) mod metrics {
    use metrics::{register_int_counter_vec, IntCounterVec};
    use once_cell::sync::Lazy;

    static S3_REQUESTS_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "remote_storage_s3_requests_count",
            "Number of s3 requests of particular type",
            &["request_type"],
        )
        .expect("failed to define a metric")
    });

    static S3_REQUESTS_FAIL_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "remote_storage_s3_failures_count",
            "Number of failed s3 requests of particular type",
            &["request_type"],
        )
        .expect("failed to define a metric")
    });

    pub fn inc_get_object() {
        S3_REQUESTS_COUNT.with_label_values(&["get_object"]).inc();
    }

    pub fn inc_get_object_fail() {
        S3_REQUESTS_FAIL_COUNT
            .with_label_values(&["get_object"])
            .inc();
    }

    pub fn inc_put_object() {
        S3_REQUESTS_COUNT.with_label_values(&["put_object"]).inc();
    }

    pub fn inc_put_object_fail() {
        S3_REQUESTS_FAIL_COUNT
            .with_label_values(&["put_object"])
            .inc();
    }

    pub fn inc_delete_object() {
        S3_REQUESTS_COUNT
            .with_label_values(&["delete_object"])
            .inc();
    }

    pub fn inc_delete_object_fail() {
        S3_REQUESTS_FAIL_COUNT
            .with_label_values(&["delete_object"])
            .inc();
    }

    pub fn inc_list_objects() {
        S3_REQUESTS_COUNT.with_label_values(&["list_objects"]).inc();
    }

    pub fn inc_list_objects_fail() {
        S3_REQUESTS_FAIL_COUNT
            .with_label_values(&["list_objects"])
            .inc();
    }
}

/// AWS S3 storage.
pub struct S3Bucket {
    client: Client,
    bucket_name: String,
    prefix_in_bucket: Option<String>,
    // Every request to S3 can be throttled or cancelled, if a certain number of requests per second is exceeded.
    // Same goes to IAM, which is queried before every S3 request, if enabled. IAM has even lower RPS threshold.
    // The helps to ensure we don't exceed the thresholds.
    concurrency_limiter: Semaphore,
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
        debug!(
            "Creating s3 remote storage for S3 bucket {}",
            aws_config.bucket_name
        );
        let mut config_builder = Config::builder()
            .region(Region::new(aws_config.bucket_region.clone()))
            .credentials_provider(provide_credentials_fn(|| async {
                match var("AWS_ACCESS_KEY_ID").is_ok() && var("AWS_SECRET_ACCESS_KEY").is_ok() {
                    true => {
                        EnvironmentVariableCredentialsProvider::new()
                            .provide_credentials()
                            .await
                    }
                    false => {
                        let imds_client = imds::Client::builder()
                            .connect_timeout(DEFAULT_IMDS_TIMEOUT)
                            .read_timeout(DEFAULT_IMDS_TIMEOUT)
                            .build()
                            .await
                            .map_err(CredentialsError::unhandled)?;
                        ImdsCredentialsProvider::builder()
                            .imds_client(imds_client)
                            .build()
                            .provide_credentials()
                            .await
                    }
                }
            }));

        if let Some(custom_endpoint) = aws_config.endpoint.clone() {
            let endpoint = Endpoint::immutable(
                custom_endpoint
                    .parse()
                    .expect("Failed to parse S3 custom endpoint"),
            );
            config_builder.set_endpoint_resolver(Some(Arc::new(endpoint)));
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
            prefix_in_bucket,
            concurrency_limiter: Semaphore::new(aws_config.concurrency_limit.get()),
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

    fn relative_path_to_s3_object(&self, path: &RemotePath) -> String {
        let mut full_path = self.prefix_in_bucket.clone().unwrap_or_default();
        for segment in path.0.iter() {
            full_path.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
            full_path.push_str(segment.to_str().unwrap_or_default());
        }
        full_path
    }

    async fn download_object(&self, request: GetObjectRequest) -> Result<Download, DownloadError> {
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 download")
            .map_err(DownloadError::Other)?;

        metrics::inc_get_object();

        let get_object = self
            .client
            .get_object()
            .bucket(request.bucket)
            .key(request.key)
            .set_range(request.range)
            .send()
            .await;

        match get_object {
            Ok(object_output) => {
                let metadata = object_output.metadata().cloned().map(StorageMetadata);
                Ok(Download {
                    metadata,
                    download_stream: Box::pin(io::BufReader::new(
                        object_output.body.into_async_read(),
                    )),
                })
            }
            Err(SdkError::ServiceError {
                err:
                    GetObjectError {
                        kind: GetObjectErrorKind::NoSuchKey(..),
                        ..
                    },
                ..
            }) => Err(DownloadError::NotFound),
            Err(e) => {
                metrics::inc_get_object_fail();
                Err(DownloadError::Other(anyhow::anyhow!(
                    "Failed to download S3 object: {e}"
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl RemoteStorage for S3Bucket {
    async fn list(&self) -> anyhow::Result<Vec<RemotePath>> {
        let mut document_keys = Vec::new();

        let mut continuation_token = None;
        loop {
            let _guard = self
                .concurrency_limiter
                .acquire()
                .await
                .context("Concurrency limiter semaphore got closed during S3 list")?;

            metrics::inc_list_objects();

            let fetch_response = self
                .client
                .list_objects_v2()
                .bucket(self.bucket_name.clone())
                .set_prefix(self.prefix_in_bucket.clone())
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|e| {
                    metrics::inc_list_objects_fail();
                    e
                })?;
            document_keys.extend(
                fetch_response
                    .contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|o| Some(self.s3_object_to_relative_path(o.key()?))),
            );

            match fetch_response.continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }

        Ok(document_keys)
    }

    /// See the doc for `RemoteStorage::list_prefixes`
    /// Note: it wont include empty "directories"
    async fn list_prefixes(&self, prefix: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        // get the passed prefix or if it is not set use prefix_in_bucket value
        let list_prefix = prefix
            .map(|p| self.relative_path_to_s3_object(p))
            .or_else(|| self.prefix_in_bucket.clone())
            .map(|mut p| {
                // required to end with a separator
                // otherwise request will return only the entry of a prefix
                if !p.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    p.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                }
                p
            });

        let mut document_keys = Vec::new();

        let mut continuation_token = None;
        loop {
            let _guard = self
                .concurrency_limiter
                .acquire()
                .await
                .context("Concurrency limiter semaphore got closed during S3 list")?;

            metrics::inc_list_objects();

            let fetch_response = self
                .client
                .list_objects_v2()
                .bucket(self.bucket_name.clone())
                .set_prefix(list_prefix.clone())
                .set_continuation_token(continuation_token)
                .delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string())
                .send()
                .await
                .map_err(|e| {
                    metrics::inc_list_objects_fail();
                    e
                })?;

            document_keys.extend(
                fetch_response
                    .common_prefixes
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|o| Some(self.s3_object_to_relative_path(o.prefix()?))),
            );

            match fetch_response.continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }

        Ok(document_keys)
    }

    async fn upload(
        &self,
        from: Box<(dyn io::AsyncRead + Unpin + Send + Sync + 'static)>,
        from_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 upload")?;

        metrics::inc_put_object();

        let body = Body::wrap_stream(ReaderStream::new(from));
        let bytes_stream = ByteStream::new(SdkBody::from(body));

        self.client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(to))
            .set_metadata(metadata.map(|m| m.0))
            .content_length(from_size_bytes.try_into()?)
            .body(bytes_stream)
            .send()
            .await
            .map_err(|e| {
                metrics::inc_put_object_fail();
                e
            })?;
        Ok(())
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        self.download_object(GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.relative_path_to_s3_object(from),
            ..GetObjectRequest::default()
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

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 delete")?;

        metrics::inc_delete_object();

        self.client
            .delete_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(path))
            .send()
            .await
            .map_err(|e| {
                metrics::inc_delete_object_fail();
                e
            })?;
        Ok(())
    }
}
