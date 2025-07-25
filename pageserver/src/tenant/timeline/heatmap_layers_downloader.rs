//! Timeline utility module to hydrate everything from the current heatmap.
//!
//! Provides utilities to spawn and abort a background task where the downloads happen.
//! See /v1/tenant/:tenant_shard_id/timeline/:timeline_id/download_heatmap_layers.

use std::sync::{Arc, Mutex};

use futures::StreamExt;
use http_utils::error::ApiError;
use tokio_util::sync::CancellationToken;
use utils::sync::gate::Gate;

use crate::context::RequestContext;

use super::Timeline;

// This status is not strictly necessary now, but gives us a nice place
// to store progress information if we ever wish to expose it.
pub(super) enum HeatmapLayersDownloadStatus {
    InProgress,
    Complete,
}

pub(super) struct HeatmapLayersDownloader {
    handle: tokio::task::JoinHandle<()>,
    status: Arc<Mutex<HeatmapLayersDownloadStatus>>,
    cancel: CancellationToken,
    downloads_guard: Arc<Gate>,
}

impl HeatmapLayersDownloader {
    fn new(
        timeline: Arc<Timeline>,
        concurrency: usize,
        recurse: bool,
        ctx: RequestContext,
    ) -> Result<HeatmapLayersDownloader, ApiError> {
        let tl_guard = timeline.gate.enter().map_err(|_| ApiError::Cancelled)?;

        let cancel = timeline.cancel.child_token();
        let downloads_guard = Arc::new(Gate::default());

        let status = Arc::new(Mutex::new(HeatmapLayersDownloadStatus::InProgress));

        let handle = tokio::task::spawn({
            let status = status.clone();
            let downloads_guard = downloads_guard.clone();
            let cancel = cancel.clone();

            async move {
                let _guard = tl_guard;

                scopeguard::defer! {
                    *status.lock().unwrap() = HeatmapLayersDownloadStatus::Complete;
                }

                let Some(heatmap) = timeline.generate_heatmap().await else {
                    tracing::info!("Heatmap layers download failed to generate heatmap");
                    return;
                };

                tracing::info!(
                    resident_size=%timeline.resident_physical_size(),
                    heatmap_layers=%heatmap.all_layers().count(),
                    "Starting heatmap layers download"
                );

                let stream = futures::stream::iter(heatmap.all_layers().cloned().filter_map(
                    |layer| {
                        let ctx = ctx.attached_child();
                        let tl = timeline.clone();
                        let dl_guard = match downloads_guard.enter() {
                            Ok(g) => g,
                            Err(_) => {
                                // [`Self::shutdown`] was called. Don't spawn any more downloads.
                                return None;
                            }
                        };

                        Some(async move {
                            let _dl_guard = dl_guard;

                            let res = tl.download_layer(&layer.name, &ctx).await;
                            if let Err(err) = res
                                && !err.is_cancelled() {
                                    tracing::warn!(layer=%layer.name,"Failed to download heatmap layer: {err}")
                                }
                        })
                    }
                )).buffered(concurrency);

                tokio::select! {
                    _ = stream.collect::<()>() => {
                        tracing::info!(
                            resident_size=%timeline.resident_physical_size(),
                            "Heatmap layers download completed"
                        );
                    },
                    _ = cancel.cancelled() => {
                        tracing::info!("Heatmap layers download cancelled");
                        return;
                    }
                }

                if recurse && let Some(ancestor) = timeline.ancestor_timeline() {
                    let ctx = ctx.attached_child();
                    let res = ancestor.start_heatmap_layers_download(concurrency, recurse, &ctx);
                    if let Err(err) = res {
                        tracing::info!(
                            "Failed to start heatmap layers download for ancestor: {err}"
                        );
                    }
                }
            }
        });

        Ok(Self {
            status,
            handle,
            cancel,
            downloads_guard,
        })
    }

    fn is_complete(&self) -> bool {
        matches!(
            *self.status.lock().unwrap(),
            HeatmapLayersDownloadStatus::Complete
        )
    }

    /// Drive any in-progress downloads to completion and stop spawning any new ones.
    ///
    /// This has two callers and they behave differently
    /// 1. [`Timeline::shutdown`]: the drain will be immediate since downloads themselves
    ///    are sensitive to timeline cancellation.
    ///
    /// 2. Endpoint handler in [`crate::http::routes`]: the drain will wait for any in-progress
    ///    downloads to complete.
    async fn stop_and_drain(self) {
        // Counterintuitive: close the guard before cancelling.
        // Something needs to poll the already created download futures to completion.
        // If we cancel first, then the underlying task exits and we lost
        // the poller.
        self.downloads_guard.close().await;
        self.cancel.cancel();
        if let Err(err) = self.handle.await {
            tracing::warn!("Failed to join heatmap layer downloader task: {err}");
        }
    }
}

impl Timeline {
    pub(crate) fn start_heatmap_layers_download(
        self: &Arc<Self>,
        concurrency: usize,
        recurse: bool,
        ctx: &RequestContext,
    ) -> Result<(), ApiError> {
        let mut locked = self.heatmap_layers_downloader.lock().unwrap();
        if locked.as_ref().map(|dl| dl.is_complete()).unwrap_or(true) {
            let dl = HeatmapLayersDownloader::new(
                self.clone(),
                concurrency,
                recurse,
                ctx.attached_child(),
            )?;
            *locked = Some(dl);
            Ok(())
        } else {
            Err(ApiError::Conflict("Already running".to_string()))
        }
    }

    pub(crate) async fn stop_and_drain_heatmap_layers_download(&self) {
        // This can race with the start of a new downloader and lead to a situation
        // where one donloader is shutting down and another one is in-flight.
        // The only impact is that we'd end up using more remote storage semaphore
        // units than expected.
        let downloader = self.heatmap_layers_downloader.lock().unwrap().take();
        if let Some(dl) = downloader {
            dl.stop_and_drain().await;
        }
    }
}
