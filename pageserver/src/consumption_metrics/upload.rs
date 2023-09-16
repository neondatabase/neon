use consumption_metrics::{idempotency_key, Event, EventChunk, CHUNK_SIZE};
use tokio_util::sync::CancellationToken;
use tracing::*;

use super::{Cache, Ids, RawMetric};

#[tracing::instrument(skip_all, fields(metrics_total = %metrics.len()))]
pub(super) async fn upload_metrics(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    cancel: &CancellationToken,
    node_id: &str,
    metrics: &[RawMetric],
    cached_metrics: &mut Cache,
) -> anyhow::Result<()> {
    use bytes::BufMut;

    let mut uploaded = 0;
    let mut failed = 0;

    let started_at = std::time::Instant::now();

    // write to a BytesMut so that we can cheaply clone the frozen Bytes for retries
    let mut buffer = bytes::BytesMut::new();
    let mut chunk_to_send = Vec::new();

    for chunk in metrics.chunks(CHUNK_SIZE) {
        chunk_to_send.clear();

        // FIXME: this should always overwrite and truncate to chunk.len()
        chunk_to_send.extend(chunk.iter().map(|(curr_key, (when, curr_val))| Event {
            kind: *when,
            metric: curr_key.metric,
            // FIXME: finally write! this to the prev allocation
            idempotency_key: idempotency_key(node_id),
            value: *curr_val,
            extra: Ids {
                tenant_id: curr_key.tenant_id,
                timeline_id: curr_key.timeline_id,
            },
        }));

        serde_json::to_writer(
            (&mut buffer).writer(),
            &EventChunk {
                events: (&chunk_to_send).into(),
            },
        )?;

        let body = buffer.split().freeze();
        let event_bytes = body.len();

        let res = upload(client, metric_collection_endpoint, body, cancel)
            .instrument(tracing::info_span!(
                "upload",
                %event_bytes,
                uploaded,
                total = metrics.len(),
            ))
            .await;

        match res {
            Ok(()) => {
                for (curr_key, curr_val) in chunk {
                    cached_metrics.insert(*curr_key, *curr_val);
                }
                uploaded += chunk.len();
            }
            Err(_) => {
                // failure(s) have already been logged
                //
                // however this is an inconsistency: if we crash here, we will start with the
                // values as uploaded. in practice, the rejections no longer happen.
                failed += chunk.len();
            }
        }
    }

    let elapsed = started_at.elapsed();

    tracing::info!(
        uploaded,
        failed,
        elapsed_ms = elapsed.as_millis(),
        "done sending metrics"
    );

    Ok(())
}

enum UploadError {
    Rejected(reqwest::StatusCode),
    Reqwest(reqwest::Error),
    Cancelled,
}

impl std::fmt::Debug for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // use same impl because backoff::retry will log this using both
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use UploadError::*;

        match self {
            Rejected(code) => write!(f, "server rejected the metrics with {code}"),
            Reqwest(e) => write!(f, "request failed: {e}"),
            Cancelled => write!(f, "cancelled"),
        }
    }
}

impl UploadError {
    fn is_reject(&self) -> bool {
        matches!(self, UploadError::Rejected(_))
    }
}

async fn upload(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    body: bytes::Bytes,
    cancel: &CancellationToken,
) -> Result<(), UploadError> {
    let warn_after = 3;
    let max_attempts = 10;
    let res = utils::backoff::retry(
        move || {
            let body = body.clone();
            async move {
                let res = client
                    .post(metric_collection_endpoint.clone())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .send()
                    .await;

                let res = res.and_then(|res| res.error_for_status());

                match res {
                    Ok(_response) => Ok(()),
                    Err(e) => {
                        let status = e.status().filter(|s| s.is_client_error());
                        if let Some(status) = status {
                            Err(UploadError::Rejected(status))
                        } else {
                            Err(UploadError::Reqwest(e))
                        }
                    }
                }
            }
        },
        UploadError::is_reject,
        warn_after,
        max_attempts,
        "upload consumption_metrics",
        utils::backoff::Cancel::new(cancel.clone(), || UploadError::Cancelled),
    )
    .await;

    match &res {
        Ok(_) => {}
        Err(e) if e.is_reject() => {
            // permanent errors currently do not get logged by backoff::retry
            // display alternate has no effect, but keeping it here for easier pattern matching.
            tracing::error!("failed to upload metrics: {e:#}");
        }
        Err(_) => {
            // these have been logged already
        }
    }

    res
}
