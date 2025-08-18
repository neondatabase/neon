#![allow(dead_code)]
#![allow(unused)]

use crate::config::GCSConfig;
use crate::error::Cancelled;
pub(super) use crate::metrics::RequestKind;
use crate::metrics::{AttemptOutcome, start_counting_cancelled_wait, start_measuring_requests};
use crate::{
    ConcurrencyLimiter, Download, DownloadError, DownloadOpts, GCS_SCOPES, Listing, ListingMode,
    ListingObject, MAX_KEYS_PER_DELETE_GCS, REMOTE_STORAGE_PREFIX_SEPARATOR, RemotePath,
    RemoteStorage, StorageMetadata, TimeTravelError, TimeoutOrCancel, Version, VersionId,
    VersionListing,
};
use anyhow::Context;
use azure_core::Etag;
use bytes::Bytes;
use bytes::BytesMut;
use chrono::DateTime;
use futures::stream::Stream;
use futures::stream::TryStreamExt;
use futures_util::StreamExt;
use gcp_auth::{Token, TokenProvider};
use http::Method;
use http::StatusCode;
use reqwest::{Client, header};
use scopeguard::ScopeGuard;
use serde::{Deserialize, Deserializer, Serialize, de};
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::{NonZeroU32, ParseIntError};
use std::pin::{Pin, pin};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::sync::CancellationToken;
use tracing;
use url::{ParseError, Url};
use utils::backoff;
use uuid::Uuid;

// ---------
pub struct GCSBucket {
    token_provider: Arc<dyn TokenProvider>,
    bucket_name: String,
    prefix_in_bucket: Option<String>,
    max_keys_per_list_response: Option<i32>,
    concurrency_limiter: ConcurrencyLimiter,
    pub timeout: Duration,
}

struct GetObjectRequest {
    bucket: String,
    key: String,
    etag: Option<String>,
    range: Option<String>,
}

// ---------

impl GCSBucket {
    pub async fn new(remote_storage_config: &GCSConfig, timeout: Duration) -> anyhow::Result<Self> {
        tracing::debug!(
            "creating remote storage for gcs bucket {}",
            remote_storage_config.bucket_name
        );

        // clean up 'prefix_in_bucket' if user provides '/pageserver' or 'pageserver/'
        let prefix_in_bucket = remote_storage_config
            .prefix_in_bucket
            .as_deref()
            .map(|prefix| {
                let mut prefix = prefix;
                while prefix.starts_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    prefix = &prefix[1..];
                }

                let mut prefix = prefix.to_string();
                if prefix.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    prefix.pop();
                }

                prefix
            });

        // get GOOGLE_APPLICATION_CREDENTIALS
        let provider = gcp_auth::provider().await?;

        Ok(GCSBucket {
            token_provider: Arc::clone(&provider),
            bucket_name: remote_storage_config.bucket_name.clone(),
            prefix_in_bucket,
            timeout,
            max_keys_per_list_response: remote_storage_config.max_keys_per_list_response,
            concurrency_limiter: ConcurrencyLimiter::new(
                remote_storage_config.concurrency_limit.get(),
            ),
        })
    }

    // convert `RemotePath` -> `String`
    pub fn relative_path_to_gcs_object(&self, path: &RemotePath) -> String {
        let path_string = path.get_path().as_str();
        match &self.prefix_in_bucket {
            Some(prefix) => prefix.clone() + "/" + path_string,
            None => path_string.to_string(),
        }
    }

    // convert `String` -> `RemotePath`
    pub fn gcs_object_to_relative_path(&self, key: &str) -> RemotePath {
        let relative_path =
            match key.strip_prefix(self.prefix_in_bucket.as_deref().unwrap_or_default()) {
                Some(stripped) => stripped,
                // we rely on GCS to return properly prefixed paths
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

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    fn max_keys_per_delete(&self) -> usize {
        MAX_KEYS_PER_DELETE_GCS
    }

    async fn permit(
        &self,
        kind: RequestKind,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, Cancelled> {
        let started_at = start_counting_cancelled_wait(kind);
        let acquire = self.concurrency_limiter.acquire(kind);

        let permit = tokio::select! {
            permit = acquire => permit.expect("semaphore is never closed"),
            _ = cancel.cancelled() => return Err(Cancelled),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);

        Ok(permit)
    }

    async fn owned_permit(
        &self,
        kind: RequestKind,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Cancelled> {
        let started_at = start_counting_cancelled_wait(kind);
        let acquire = self.concurrency_limiter.acquire_owned(kind);

        let permit = tokio::select! {
            permit = acquire => permit.expect("semaphore is never closed"),
            _ = cancel.cancelled() => return Err(Cancelled),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);
        Ok(permit)
    }

    async fn list_object_versions(
        &self,
        prefix: Option<&RemotePath>,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
    ) -> Result<GCSObject, DownloadError> {
        todo!();
    }

    async fn list_versions_with_permit(
        &self,
        _permit: &tokio::sync::SemaphorePermit<'_>,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> Result<crate::VersionListing, DownloadError> {
        todo!();
    }

    async fn put_object(
        &self,
        byte_stream: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        fs_size: usize,
        to: &RemotePath,
        cancel: &CancellationToken,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Put;
        let _permit = self.permit(kind, cancel).await?;
        let started_at = start_measuring_requests(kind);

        let multipart_uri = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=multipart",
            self.bucket_name.clone()
        );

        let mut metadata = metadata.clone();
        let gcs_path = self.relative_path_to_gcs_object(to);

        // Always specify destination via `RemotePath` in multipart uploads
        if metadata.is_none() {
            metadata = Some(StorageMetadata::from([("name", gcs_path.as_str())]));
        } else {
            metadata
                .as_mut()
                .map(|m| m.0.insert("name".to_string(), gcs_path));
        }

        let metadata_body = serde_json::to_string(&metadata.map(|m| m.0))?;
        let metadata_part = reqwest::multipart::Part::text(metadata_body)
            .mime_str("application/json; charset=UTF-8")?;

        let stream_body = reqwest::Body::wrap_stream(byte_stream);
        let data_part = reqwest::multipart::Part::stream_with_length(stream_body, fs_size as u64)
            .mime_str("application/octet-stream")?;

        let mut form = reqwest::multipart::Form::new()
            .part("metadata", metadata_part)
            .part("bodystream", data_part);

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_str(&format!(
                "multipart/related; boundary={}",
                form.boundary()
            ))?,
        );

        let upload = Client::new()
            .post(multipart_uri)
            .bearer_auth(self.token_provider.token(GCS_SCOPES).await?.as_str())
            .multipart(form)
            .headers(headers)
            .send();

        let upload = tokio::time::timeout(self.timeout, upload);

        let res = tokio::select! {
            res = upload => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        if let Ok(inner) = &res {
            let started_at = ScopeGuard::into_inner(started_at);
            crate::metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, inner, started_at);
        }
        match res {
            Ok(Ok(res)) => {
                if !res.status().is_success() {
                    match res.status() {
                        _ => Err(anyhow::anyhow!("GCS PUT error \n\t {:?}", res)),
                    }
                } else {
                    let body = res
                        .text()
                        .await
                        .map_err(|e: reqwest::Error| DownloadError::Other(e.into()))?;

                    let resp: GCSObject = serde_json::from_str(&body)
                        .map_err(|e: serde_json::Error| DownloadError::Other(e.into()))?;

                    if !resp.size.is_some_and(|s| s == fs_size as i64) {
                        return Err(anyhow::anyhow!(
                            "Buy a lottery ticket. Boundary string from 'multipart/related' HTTP upload occurred in payload"
                        ));
                    };

                    Ok(())
                }
            }
            Ok(Err(reqw)) => Err(reqw.into()),
            Err(_timeout) => Err(TimeoutOrCancel::Timeout.into()),
        }
    }

    async fn copy(
        &self,
        from: String,
        to: String,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Copy;

        let _permit = self.permit(kind, cancel).await?;

        let timeout = tokio::time::sleep(self.timeout);

        let started_at = start_measuring_requests(kind);

        let copy_uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}/copyTo/b/{}/o/{}",
            self.bucket_name.clone(),
            &from,
            self.bucket_name.clone(),
            &to
        );

        let op = Client::new()
            .post(copy_uri)
            .bearer_auth(self.token_provider.token(GCS_SCOPES).await?.as_str())
            .send();

        let res = tokio::select! {
            res = op => res,
            _ = timeout => return Err(TimeoutOrCancel::Timeout.into()),
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        res?;

        Ok(())
    }

    async fn delete_oids(
        &self,
        delete_objects: &[String],
        cancel: &CancellationToken,
        _permit: &tokio::sync::SemaphorePermit<'_>,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let mut cancel = std::pin::pin!(cancel.cancelled());

        for chunk in delete_objects.chunks(MAX_KEYS_PER_DELETE_GCS) {
            let started_at = start_measuring_requests(kind);

            // Use this to report keys that didn't delete based on 'content_id'
            let mut delete_objects_status = HashMap::new();

            let mut form = reqwest::multipart::Form::new();
            let bulk_uri = "https://storage.googleapis.com/batch/storage/v1";

            for (index, path) in delete_objects.iter().enumerate() {
                delete_objects_status.insert(index + 1, path.clone());

                let path_to_delete: String =
                    url::form_urlencoded::byte_serialize(path.trim_start_matches("/").as_bytes())
                        .collect();

                let delete_req = format!(
                    "
                    DELETE /storage/v1/b/{}/o/{} HTTP/1.1\r\n\
                    Content-Type: application/json\r\n\
                    accept: application/json\r\n\
                    content-length: 0\r\n
                    ",
                    self.bucket_name.clone(),
                    path_to_delete
                )
                .trim()
                .to_string();

                let content_id = format!("<{}+{}>", Uuid::new_v4(), index + 1);

                let mut part_headers = header::HeaderMap::new();
                part_headers.insert(
                    header::CONTENT_TYPE,
                    header::HeaderValue::from_static("application/http"),
                );
                part_headers.insert(
                    header::TRANSFER_ENCODING,
                    header::HeaderValue::from_static("binary"),
                );
                part_headers.insert(
                    header::HeaderName::from_static("content-id"),
                    header::HeaderValue::from_str(&content_id)?,
                );
                let part = reqwest::multipart::Part::text(delete_req).headers(part_headers);

                form = form.part(format!("request-{}", index), part);
            }

            let mut headers = header::HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_str(&format!(
                    "multipart/mixed; boundary={}",
                    form.boundary()
                ))?,
            );

            let req = Client::new()
                .post(bulk_uri)
                .bearer_auth(self.token_provider.token(GCS_SCOPES).await?.as_str())
                .multipart(form)
                .headers(headers)
                .send();

            let resp = tokio::select! {
                resp = req => resp,
                _ = tokio::time::sleep(self.timeout) => return Err(TimeoutOrCancel::Timeout.into()),
                _ = &mut cancel => return Err(TimeoutOrCancel::Cancel.into()),
            };

            let started_at = ScopeGuard::into_inner(started_at);
            crate::metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &resp, started_at);

            let resp = resp.context("request deletion")?;

            crate::metrics::BUCKET_METRICS
                .deleted_objects_total
                .inc_by(chunk.len() as u64);

            let res_headers = resp.headers().to_owned();

            let boundary = res_headers
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()?
                .split("=")
                .last()
                .unwrap();

            let res_body = resp.text().await?;

            let parsed: HashMap<String, String> = res_body
                .split(&format!("--{}", boundary))
                .filter_map(|c| {
                    let mut lines = c.lines();

                    let id = lines.find_map(|line| {
                        line.strip_prefix("Content-ID:")
                            .and_then(|suf| suf.split('+').last())
                            .and_then(|suf| suf.split('>').next())
                            .map(|x| x.trim().to_string())
                    });

                    let status_code = lines.find_map(|line| {
                        // Not sure if this protocol version shouldn't be so specific
                        line.strip_prefix("HTTP/1.1")
                            .and_then(|x| x.split_whitespace().next())
                            .map(|x| x.trim().to_string())
                    });

                    id.zip(status_code)
                })
                .collect();

            // Gather failures
            let errors: HashMap<usize, &String> = parsed
                .iter()
                .filter_map(|(x, y)| {
                    let id = x.parse::<usize>().ok();
                    if y == "404" {
                        // GCS returns Error on 404, S3 doesn't. Warn and omit from failed count.
                        // https://cloud.google.com/storage/docs/xml-api/delete-object
                        tracing::warn!(
                            "DeleteObjects key {} {} NotFound. Already deleted.",
                            delete_objects_status.get(&id?).unwrap(),
                            y
                        );
                        None
                    } else if y.chars().next() != Some('2') {
                        id.map(|v| (v, y))
                    } else {
                        None
                    }
                })
                .collect();

            if !errors.is_empty() {
                // Report 10 of them like S3
                const LOG_UP_TO_N_ERRORS: usize = 10;
                for (id, code) in errors.iter().take(LOG_UP_TO_N_ERRORS) {
                    tracing::warn!(
                        "DeleteObjects key {} failed with code: {}",
                        delete_objects_status.get(id).unwrap(),
                        code
                    );
                }

                return Err(anyhow::anyhow!(
                    "Failed to delete {}/{} objects",
                    errors.len(),
                    chunk.len(),
                ));
            }
        }

        Ok(())
    }

    async fn head_object(
        &self,
        key: String,
        cancel: &CancellationToken,
    ) -> Result<GCSObject, DownloadError> {
        let kind = RequestKind::Head;
        let _permit = self.permit(kind, cancel).await?;

        let encoded_path: String = url::form_urlencoded::byte_serialize(key.as_bytes()).collect();

        let metadata_uri_mod = "alt=json";
        let download_uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}?{}",
            self.bucket_name.clone(),
            encoded_path,
            metadata_uri_mod
        );

        let head_future = Client::new()
            .get(download_uri)
            .bearer_auth(
                self.token_provider
                    .token(GCS_SCOPES)
                    .await
                    .map_err(|e: gcp_auth::Error| DownloadError::Other(e.into()))?
                    .as_str(),
            )
            .send();

        let started_at = start_measuring_requests(kind);

        let head_future = tokio::time::timeout(self.timeout, head_future);

        let res = tokio::select! {
            res = head_future => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        let res = res.map_err(|_e| DownloadError::Timeout)?;

        // do not incl. timeouts as errors in metrics but cancellations
        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        let data = match res {
            Ok(data) => {
                if !data.status().is_success() {
                    match data.status() {
                        StatusCode::NOT_FOUND => return Err(DownloadError::NotFound),
                        _ => {
                            return Err(DownloadError::Other(anyhow::anyhow!(
                                "GCS head response contained no response body"
                            )));
                        }
                    }
                } else {
                    data
                }
            }
            Err(e) => {
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Err,
                    started_at,
                );

                return Err(DownloadError::Other(
                    anyhow::Error::new(e).context("error in HEAD of GCS object"),
                ));
            }
        };

        let body = data
            .text()
            .await
            .map_err(|e: reqwest::Error| DownloadError::Other(e.into()))?;

        let resp: GCSObject = serde_json::from_str(&body)
            .map_err(|e: serde_json::Error| DownloadError::Other(e.into()))?;

        Ok(resp)
    }

    async fn list_objects_v2(&self, list_uri: String) -> anyhow::Result<reqwest::RequestBuilder> {
        let res = Client::new()
            .get(list_uri)
            .bearer_auth(self.token_provider.token(GCS_SCOPES).await?.as_str());
        Ok(res)
    }

    // need a 'bucket', a 'key', and a bytes 'range'.
    async fn get_object(
        &self,
        request: GetObjectRequest,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Download, DownloadError> {
        let kind = RequestKind::Get;

        let permit = self.owned_permit(kind, cancel).await?;

        let started_at = start_measuring_requests(kind);

        let encoded_path: String =
            url::form_urlencoded::byte_serialize(request.key.as_bytes()).collect();

        /// We do this in two parts:
        /// 1. Serialize the metadata of the first request to get Etag, last modified, etc
        /// 2. We do not .await the second request pass on the pinned stream to the 'get_object'
        ///    caller
        // 1. Serialize Metadata in initial request
        let metadata_uri_mod = "alt=json";
        let download_uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}?{}",
            self.bucket_name.clone(),
            encoded_path,
            metadata_uri_mod
        );

        let res = Client::new()
            .get(download_uri)
            .bearer_auth(
                self.token_provider
                    .token(GCS_SCOPES)
                    .await
                    .map_err(|e: gcp_auth::Error| DownloadError::Other(e.into()))?
                    .as_str(),
            )
            .send();

        let obj_metadata = tokio::select! {
            res = res => res,
            _ = tokio::time::sleep(self.timeout) => return Err(DownloadError::Timeout),
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
        };

        let resp = match obj_metadata {
            Ok(resp) => {
                if !resp.status().is_success() {
                    match resp.status() {
                        StatusCode::NOT_FOUND => return Err(DownloadError::NotFound),
                        _ => {
                            return Err(DownloadError::Other(anyhow::anyhow!(
                                "GCS GET resposne contained no response body"
                            )));
                        }
                    }
                } else {
                    resp
                }
            }
            _ => {
                return Err(DownloadError::Other(anyhow::anyhow!("download gcs object")));
            }
        };

        let body = resp
            .text()
            .await
            .map_err(|e: reqwest::Error| DownloadError::Other(e.into()))?;

        let resp: GCSObject = serde_json::from_str(&body)
            .map_err(|e: serde_json::Error| DownloadError::Other(e.into()))?;

        // 2. Byte Stream request
        let mut headers = header::HeaderMap::new();
        headers.insert(header::RANGE, header::HeaderValue::from_static("bytes=0-"));

        let encoded_path: String =
            url::form_urlencoded::byte_serialize(request.key.as_bytes()).collect();

        let stream_uri_mod = "alt=media";
        // See: https://cloud.google.com/storage/docs/streaming-downloads#stream_a_download
        // REST APIs > JSON API > 1st bullet  point
        let generation = resp
            .generation
            .expect("object did not contain generation number");
        let generation_mod = format!("generation={generation}");
        let stream_uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}?{}&{}",
            self.bucket_name.clone(),
            encoded_path,
            stream_uri_mod,
            generation_mod,
        );

        let mut req = Client::new()
            .get(stream_uri)
            .headers(headers)
            .bearer_auth(
                self.token_provider
                    .token(GCS_SCOPES)
                    .await
                    .map_err(|e: gcp_auth::Error| DownloadError::Other(e.into()))?
                    .as_str(),
            )
            .send();

        let get_object = tokio::select! {
            res = req => res,
            _ = tokio::time::sleep(self.timeout) => return Err(DownloadError::Timeout),
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
        };

        let started_at = ScopeGuard::into_inner(started_at);

        let object_output = match get_object {
            Ok(object_output) => {
                if !object_output.status().is_success() {
                    match object_output.status() {
                        StatusCode::NOT_FOUND => return Err(DownloadError::NotFound),
                        _ => {
                            return Err(DownloadError::Other(anyhow::anyhow!(
                                "GCS GET response contained no response body"
                            )));
                        }
                    }
                } else {
                    object_output
                }
            }
            Err(e) => {
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Err,
                    started_at,
                );

                return Err(DownloadError::Other(
                    anyhow::Error::new(e).context("download s3 object"),
                ));
            }
        };

        let remaining = self.timeout.saturating_sub(started_at.elapsed());

        let metadata = resp.metadata.map(StorageMetadata);

        let etag = resp
            .etag
            .ok_or(DownloadError::Other(anyhow::anyhow!("Missing ETag header")))?
            .into();

        let last_modified: SystemTime = resp
            .updated
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|s| s.into())
            .unwrap_or(SystemTime::now());

        // But let data stream pass through
        Ok(Download {
            download_stream: Box::pin(object_output.bytes_stream().map(|item| {
                item.map_err(|e: reqwest::Error| std::io::Error::new(std::io::ErrorKind::Other, e))
            })),
            etag,
            last_modified,
            metadata,
        })
    }
}

impl RemoteStorage for GCSBucket {
    // ---------------------------------------
    // Neon wrappers for GCS client functions
    // ---------------------------------------

    fn list_streaming(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> impl Stream<Item = Result<Listing, DownloadError>> {
        let kind = RequestKind::List;

        let mut max_keys = max_keys.map(|mk| mk.get() as i32);

        let list_prefix = prefix
            .map(|p| self.relative_path_to_gcs_object(p))
            .or_else(|| {
                self.prefix_in_bucket.clone().map(|mut s| {
                    s.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                    s
                })
            })
            .unwrap();

        let request_max_keys = self
            .max_keys_per_list_response
            .into_iter()
            .chain(max_keys.into_iter())
            .min()
            // https://cloud.google.com/storage/docs/json_api/v1/objects/list?hl=en#parameters
            // TODO set this to default
            .unwrap_or(1000);

        // We pass URI in to `list_objects_v2` as we'll modify it with `NextPageToken`, hence
        // `mut`
        let mut list_uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}&maxResults={}",
            self.bucket_name.clone(),
            list_prefix,
            request_max_keys,
        );

        // on ListingMode:
        // https://github.com/neondatabase/neon/blob/edc11253b65e12a10843711bd88ad277511396d7/libs/remote_storage/src/lib.rs#L158C1-L164C2
        if let ListingMode::WithDelimiter = mode {
            list_uri.push_str(&format!(
                "&delimiter={}",
                REMOTE_STORAGE_PREFIX_SEPARATOR.to_string()
            ));
        }

        async_stream::stream! {

            let mut continuation_token = None;

            'outer: loop {
                let started_at = start_measuring_requests(kind);

                let request = self.list_objects_v2(list_uri.clone())
                    .await
                    .map_err(DownloadError::Other)?
                    .send();

                // this is like `await`
                let response = tokio::select! {
                    res = request => Ok(res),
                    _ = tokio::time::sleep(self.timeout) => Err(DownloadError::Timeout),
                    _ = cancel.cancelled() => Err(DownloadError::Cancelled),
                }?;

                // just mapping our `Result' error variant's type.
                let response = response
                    .context("Failed to list GCS prefixes")
                    .map_err(DownloadError::Other);

                let started_at = ScopeGuard::into_inner(started_at);

                crate::metrics::BUCKET_METRICS
                    .req_seconds
                    .observe_elapsed(kind, &response, started_at);

                let response = match response {
                    Ok(response) => response,
                    Err(e) => {
                        // The error is potentially retryable, so we must rewind the loop after yielding.
                        yield Err(e);
                        continue 'outer;
                    },
                };

                let body = response.text()
                    .await
                    .map_err(|e: reqwest::Error| DownloadError::Other(e.into()))?;

                let resp: GCSListResponse = serde_json::from_str(&body).map_err(|e: serde_json::Error| DownloadError::Other(e.into()))?;

                let prefixes = resp.common_prefixes();
                let keys = resp.contents();

                tracing::debug!("list: {} prefixes, {} keys", prefixes.len(), keys.len());

                let mut result = Listing::default();

                for res in keys.iter() {
                   let last_modified: SystemTime = res.updated.clone()
                       .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                       .map(|s| s.into())
                       .unwrap_or(SystemTime::now());

                   let size = res.size.unwrap_or(0) as u64;

                   let key = res.name.clone();

                   result.keys.push(
                        ListingObject{
                            key: self.gcs_object_to_relative_path(&key),
                            last_modified,
                            size
                        }
                   );

                   if let Some(mut mk) = max_keys {
                       assert!(mk > 0);
                       mk -= 1;
                       if mk == 0 {
                          tracing::debug!("reached limit set by max_keys");
                          yield Ok(result);
                          break 'outer;
                       }
                       max_keys = Some(mk);
                   };
                }

                result.prefixes.extend(prefixes.iter().filter_map(|p| {
                    Some(
                        self.gcs_object_to_relative_path(
                            p.trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR)
                        ),
                    )
                }));

                yield Ok(result);

                continuation_token = match resp.next_page_token {
                    Some(token) => {
                        list_uri = list_uri + "&pageToken=" + &token;
                        Some(token)
                    },
                    None => break
                }
            }
        }
    }

    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Put;
        let _permit = self.permit(kind, cancel).await?;

        let started_at = start_measuring_requests(kind);

        let upload = self.put_object(from, from_size_bytes, to, cancel, metadata);

        let upload = tokio::time::timeout(self.timeout, upload);

        let res = tokio::select! {
            res = upload => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        if let Ok(inner) = &res {
            // do not incl. timeouts as errors in metrics but cancellations
            let started_at = ScopeGuard::into_inner(started_at);
            crate::metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, inner, started_at);
        }

        match res {
            Ok(Ok(_put)) => Ok(()),
            Ok(Err(sdk)) => {
                println!("{:?}", sdk);
                Err(sdk.into())
            }
            Err(_timeout) => Err(TimeoutOrCancel::Timeout.into()),
        }
    }

    async fn copy(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Copy;
        let _permit = self.permit(kind, cancel).await?;

        let timeout = tokio::time::sleep(self.timeout);

        let started_at = start_measuring_requests(kind);

        // we need to specify bucket_name as a prefix
        let copy_source = format!(
            "{}/{}",
            self.bucket_name,
            self.relative_path_to_gcs_object(from)
        );

        todo!();
    }

    async fn download(
        &self,
        from: &RemotePath,
        opts: &DownloadOpts,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        // if prefix is not none then download file `prefix/from`
        // if prefix is none then download file `from`

        self.get_object(
            GetObjectRequest {
                bucket: self.bucket_name.clone(),
                key: self
                    .relative_path_to_gcs_object(from)
                    .trim_start_matches("/")
                    .to_string(),
                etag: opts.etag.as_ref().map(|e| e.to_string()),
                range: opts.byte_range_header(),
            },
            cancel,
        )
        .await
    }

    async fn delete_objects(
        &self,
        paths: &[RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let permit = self.permit(kind, cancel).await?;

        let mut delete_objects: Vec<String> = Vec::with_capacity(paths.len());

        let delete_objects: Vec<String> = paths
            .iter()
            .map(|i| self.relative_path_to_gcs_object(i))
            .collect();

        self.delete_oids(&delete_objects, cancel, &permit).await
    }

    fn max_keys_per_delete(&self) -> usize {
        MAX_KEYS_PER_DELETE_GCS
    }

    async fn delete(&self, path: &RemotePath, cancel: &CancellationToken) -> anyhow::Result<()> {
        let paths = std::array::from_ref(path);
        self.delete_objects(paths, cancel).await
    }

    async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
        complexity_limit: Option<NonZeroU32>,
    ) -> Result<(), TimeTravelError> {
        Ok(())
    }

    async fn head_object(
        &self,
        key: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<ListingObject, DownloadError> {
        let path = self
            .relative_path_to_gcs_object(key)
            .trim_start_matches("/")
            .to_string();

        let resp = self.head_object(path.clone(), cancel).await?;

        let last_modified: SystemTime = resp
            .updated
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|s| s.into())
            .unwrap_or(SystemTime::now());

        let Some(size) = resp.size else {
            return Err(DownloadError::Other(anyhow::anyhow!(
                "Missing size (content length) header"
            )));
        };

        Ok(ListingObject {
            key: self.gcs_object_to_relative_path(&path),
            last_modified,
            size: size as u64,
        })
    }

    async fn list_versions(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> Result<crate::VersionListing, DownloadError> {
        let kind = RequestKind::ListVersions;
        let permit = self.permit(kind, cancel).await?;
        self.list_versions_with_permit(&permit, prefix, mode, max_keys, cancel)
            .await
    }
}

// ---------

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct GCSListResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: Option<String>,
    pub items: Option<Vec<GCSObject>>,
    pub prefixes: Option<Vec<String>>,
}

fn de_from_str<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    match s {
        Some(s) => i64::from_str(&s).map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct GCSObject {
    pub name: String,
    pub bucket: String,
    #[serde(deserialize_with = "de_from_str")]
    pub generation: Option<i64>,
    pub metageneration: String,
    #[serde(rename = "contentType")]
    pub content_type: Option<String>,
    #[serde(rename = "storageClass")]
    pub storage_class: String,
    #[serde(deserialize_with = "de_from_str")]
    pub size: Option<i64>,
    #[serde(rename = "md5Hash")]
    pub md5_hash: Option<String>,
    pub crc32c: String,
    pub etag: Option<String>,
    #[serde(rename = "timeCreated")]
    pub time_created: String,
    pub updated: Option<String>,
    #[serde(rename = "timeStorageClassUpdated")]
    pub time_storage_class_updated: String,
    #[serde(rename = "timeDeleted")]
    pub time_deleted: Option<String>,
    #[serde(rename = "timeFinalized")]
    pub time_finalized: String,
    pub metadata: Option<HashMap<String, String>>,
}

impl GCSListResponse {
    pub fn contents(&self) -> &[GCSObject] {
        self.items.as_deref().unwrap_or_default()
    }
    pub fn common_prefixes(&self) -> &[String] {
        self.prefixes.as_deref().unwrap_or_default()
    }
}
