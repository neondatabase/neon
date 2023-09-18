use consumption_metrics::{Event, EventChunk, IdempotencyKey, CHUNK_SIZE};
use serde_with::serde_as;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{metrics::Name, Cache, MetricsKey, RawMetric};
use utils::id::{TenantId, TimelineId};

/// How the metrics from pageserver are identified.
#[serde_with::serde_as]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
struct Ids {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub(super) tenant_id: TenantId,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) timeline_id: Option<TimelineId>,
}

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

    let mut iter = serialize_in_chunks(CHUNK_SIZE, metrics, node_id);

    while let Some(res) = iter.next() {
        let (chunk, body) = res?;

        let event_bytes = body.len();

        let is_last = iter.len() == 0;

        let res = upload(client, metric_collection_endpoint, body, cancel, is_last)
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
fn serialize_in_chunks<'a, F>(
    chunk_size: usize,
    input: &'a [RawMetric],
    factory: F,
) -> impl ExactSizeIterator<Item = Result<(&'a [RawMetric], bytes::Bytes), serde_json::Error>> + 'a
where
    F: KeyGen<'a> + 'a,
{
    use bytes::BufMut;

    struct Iter<'a, F> {
        inner: std::slice::Chunks<'a, RawMetric>,
        chunk_size: usize,

        // write to a BytesMut so that we can cheaply clone the frozen Bytes for retries
        buffer: bytes::BytesMut,
        // chunk amount of events are reused to produce the serialized document
        scratch: Vec<Event<Ids, Name>>,
        factory: F,
    }

    impl<'a, F: KeyGen<'a>> Iterator for Iter<'a, F> {
        type Item = Result<(&'a [RawMetric], bytes::Bytes), serde_json::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            let chunk = self.inner.next()?;

            if self.scratch.is_empty() {
                // first round: create events with N strings
                self.scratch.extend(
                    chunk
                        .iter()
                        .map(|raw_metric| raw_metric.as_event(&self.factory.generate())),
                );
            } else {
                // next rounds: update_in_place to reuse allocations
                assert_eq!(self.scratch.len(), self.chunk_size);
                self.scratch
                    .iter_mut()
                    .zip(chunk.iter())
                    .for_each(|(slot, raw_metric)| {
                        raw_metric.update_in_place(slot, &self.factory.generate())
                    });
            }

            let res = serde_json::to_writer(
                (&mut self.buffer).writer(),
                &EventChunk {
                    events: (&self.scratch[..chunk.len()]).into(),
                },
            );

            match res {
                Ok(()) => Some(Ok((chunk, self.buffer.split().freeze()))),
                Err(e) => Some(Err(e)),
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.inner.size_hint()
        }
    }

    impl<'a, F: KeyGen<'a>> ExactSizeIterator for Iter<'a, F> {}

    let buffer = bytes::BytesMut::new();
    let inner = input.chunks(chunk_size);
    let scratch = Vec::new();

    Iter {
        inner,
        chunk_size,
        buffer,
        scratch,
        factory,
    }
}

trait RawMetricExt {
    fn as_event(&self, key: &IdempotencyKey<'_>) -> Event<Ids, Name>;
    fn update_in_place(&self, event: &mut Event<Ids, Name>, key: &IdempotencyKey<'_>);
}

impl RawMetricExt for RawMetric {
    fn as_event(&self, key: &IdempotencyKey<'_>) -> Event<Ids, Name> {
        let MetricsKey {
            metric,
            tenant_id,
            timeline_id,
        } = self.0;

        let (kind, value) = self.1;

        Event {
            kind,
            metric,
            idempotency_key: key.to_string(),
            value,
            extra: Ids {
                tenant_id,
                timeline_id,
            },
        }
    }

    fn update_in_place(&self, event: &mut Event<Ids, Name>, key: &IdempotencyKey<'_>) {
        use std::fmt::Write;

        let MetricsKey {
            metric,
            tenant_id,
            timeline_id,
        } = self.0;

        let (kind, value) = self.1;

        *event = Event {
            kind,
            metric,
            idempotency_key: {
                event.idempotency_key.clear();
                write!(event.idempotency_key, "{key}").unwrap();
                std::mem::take(&mut event.idempotency_key)
            },
            value,
            extra: Ids {
                tenant_id,
                timeline_id,
            },
        };
    }
}

trait KeyGen<'a>: Copy {
    fn generate(&self) -> IdempotencyKey<'a>;
}

impl<'a> KeyGen<'a> for &'a str {
    fn generate(&self) -> IdempotencyKey<'a> {
        IdempotencyKey::generate(self)
    }
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

// this is consumed by the test verifiers
static LAST_IN_BATCH: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("pageserver-metrics-last-upload-in-batch");

async fn upload(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    body: bytes::Bytes,
    cancel: &CancellationToken,
    is_last: bool,
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
                    .header(
                        LAST_IN_BATCH.clone(),
                        if is_last { "true" } else { "false" },
                    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use once_cell::sync::Lazy;

    #[test]
    fn chunked_serialization() {
        let examples = metric_samples();
        assert!(examples.len() > 1);

        let factory = FixedGen::new(Utc::now(), "1", 42);

        // need to use Event here because serde_json::Value uses default hashmap, not linked
        // hashmap
        #[derive(serde::Deserialize)]
        struct EventChunk {
            events: Vec<Event<Ids, Name>>,
        }

        let correct = serialize_in_chunks(examples.len(), &examples, factory)
            .map(|res| res.unwrap().1)
            .flat_map(|body| serde_json::from_slice::<EventChunk>(&body).unwrap().events)
            .collect::<Vec<_>>();

        for chunk_size in 1..examples.len() {
            let actual = serialize_in_chunks(chunk_size, &examples, factory)
                .map(|res| res.unwrap().1)
                .flat_map(|body| serde_json::from_slice::<EventChunk>(&body).unwrap().events)
                .collect::<Vec<_>>();

            // if these are equal, it means that multi-chunking version works as well
            assert_eq!(correct, actual);
        }
    }

    #[derive(Clone, Copy)]
    struct FixedGen<'a>(chrono::DateTime<chrono::Utc>, &'a str, u16);

    impl<'a> FixedGen<'a> {
        fn new(now: chrono::DateTime<chrono::Utc>, node_id: &'a str, nonce: u16) -> Self {
            FixedGen(now, node_id, nonce)
        }
    }

    impl<'a> KeyGen<'a> for FixedGen<'a> {
        fn generate(&self) -> IdempotencyKey<'a> {
            IdempotencyKey::for_tests(self.0, self.1, self.2)
        }
    }

    static SAMPLES_NOW: Lazy<DateTime<Utc>> = Lazy::new(|| {
        DateTime::parse_from_rfc3339("2023-09-15T00:00:00.123456789Z")
            .unwrap()
            .into()
    });

    #[test]
    fn metric_image_stability() {
        // it is important that these strings stay as they are

        let examples = [
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"written_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                r#"{"type":"incremental","start_time":"2023-09-14T00:00:00.123456789Z","stop_time":"2023-09-15T00:00:00.123456789Z","metric":"written_data_bytes_delta","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"timeline_logical_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"remote_storage_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"resident_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"synthetic_storage_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":1,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
        ];

        let idempotency_key = consumption_metrics::IdempotencyKey::for_tests(*SAMPLES_NOW, "1", 0);
        let examples = examples.into_iter().zip(metric_samples());

        for ((line, expected), (key, (kind, value))) in examples {
            let e = consumption_metrics::Event {
                kind,
                metric: key.metric,
                idempotency_key: idempotency_key.to_string(),
                value,
                extra: Ids {
                    tenant_id: key.tenant_id,
                    timeline_id: key.timeline_id,
                },
            };
            let actual = serde_json::to_string(&e).unwrap();
            assert_eq!(expected, actual, "example for {kind:?} from line {line}");
        }
    }

    fn metric_samples() -> [RawMetric; 6] {
        let tenant_id = TenantId::from_array([0; 16]);
        let timeline_id = TimelineId::from_array([0xff; 16]);

        let before = DateTime::parse_from_rfc3339("2023-09-14T00:00:00.123456789Z")
            .unwrap()
            .into();
        let [now, before] = [*SAMPLES_NOW, before];

        super::super::metrics::metric_examples(tenant_id, timeline_id, now, before)
    }
}
