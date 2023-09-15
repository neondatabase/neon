use consumption_metrics::{EventChunk, IdempotencyKey, CHUNK_SIZE};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{Cache, RawMetric, RawMetricExt};

#[tracing::instrument(skip_all, fields(metrics_total = %metrics.len()))]
pub(super) async fn upload_metrics(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    cancel: &CancellationToken,
    node_id: &str,
    metrics: &[RawMetric],
    cached_metrics: &mut Cache,
) -> anyhow::Result<()> {
    let mut uploaded = 0;
    let mut failed = 0;

    let started_at = std::time::Instant::now();

    for res in serialize_in_chunks(CHUNK_SIZE, metrics, node_id) {
        let (chunk, body) = res?;

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

// The return type is quite ugly, but we gain testability in isolation
fn serialize_in_chunks<'a>(
    chunk_size: usize,
    input: &'a [RawMetric],
    node_id: &'a str,
) -> impl Iterator<Item = Result<(&'a [RawMetric], bytes::Bytes), serde_json::Error>> + 'a {
    use bytes::BufMut;

    // write to a BytesMut so that we can cheaply clone the frozen Bytes for retries
    let mut buffer = bytes::BytesMut::new();
    let mut chunks = input.chunks(chunk_size);
    let mut events = Vec::new();

    std::iter::from_fn(move || {
        let chunk = chunks.next()?;

        if !events.is_empty() {
            assert_eq!(events.len(), chunk_size);
            events
                .iter_mut()
                .zip(chunk.iter())
                .for_each(|(slot, raw_metric)| {
                    raw_metric.update_in_place(slot, IdempotencyKey::generate(node_id))
                });
        } else {
            events.extend(
                chunk
                    .iter()
                    .map(|raw_metric| raw_metric.as_event(IdempotencyKey::generate(node_id))),
            );
        }

        let res = serde_json::to_writer(
            (&mut buffer).writer(),
            &EventChunk {
                events: (&events[..chunk.len()]).into(),
            },
        );

        match res {
            Ok(()) => Some(Ok((chunk, buffer.split().freeze()))),
            Err(e) => Some(Err(e)),
        }
    })
}

#[derive(thiserror::Error, Debug)]
enum UploadError {
    #[error("server rejected metrics")]
    Rejected(reqwest::StatusCode),
    #[error("request failed")]
    Reqwest(#[source] reqwest::Error),
    #[error("cancelled")]
    Cancelled,
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
    if cancel.is_cancelled() {
        // retry does not drop the request when cancel comes, so pre-check it.
        //
        // it's not terrible, because those come at shutdown when we will have very little time to
        // act anyways.
        Err(UploadError::Cancelled)
    } else {
        let warn_after = 3;
        let max_attempts = 10;
        utils::backoff::retry(
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

                    // 10 redirects are normally allowed, so we don't need worry about 3xx
                    match res {
                        Ok(_response) => Ok(()),
                        Err(e) => {
                            let status = e.status().filter(|s| s.is_client_error());
                            if let Some(status) = status {
                                // rejection used to be a thing when the server could reject a
                                // whole batch of metrics if one metric was bad.
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
        .await
    }
}
