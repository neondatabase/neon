//! TODO kb

use std::{
    ops::Deref,
    sync::{Arc, Weak},
    time::Duration,
};

use anyhow::Context;
use pageserver_api::models::{DownloadRemoteLayersTaskInfo, TimelineState};
use tokio::sync::oneshot;
use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::{pgdatadir_mapping::CalculateLogicalSizeError, tenant::storage_layer::RemoteLayer};

use super::Timeline;

/// A timeline wrapper, that ensures we can drop timeline and cause
/// next operations that access the timeline to cancel.
#[derive(Clone)]
pub struct TimelineRef {
    timeline: Weak<Timeline>,
    pub id: TenantTimelineId,
}

/// This is a struct that wraps `Arc<Timeline>` and timeline's API to
///
///
/// Use this as a `MutexGuard` and do not try to store the object.
/// While the object is held, the timeline is not dropped fully, which might cause
/// various jobs to continue working.
pub struct TimelineGuard {
    timeline: Arc<Timeline>,
}

impl TimelineGuard {
    pub fn get_current_logical_size(&self) -> anyhow::Result<(u64, bool)> {
        Timeline::get_current_logical_size(&self.timeline)
    }

    pub async fn spawn_download_all_remote_layers(
        &self,
    ) -> Result<DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskInfo> {
        Timeline::spawn_download_all_remote_layers(&self.timeline).await
    }

    pub async fn download_remote_layer(
        &self,
        remote_layer: Arc<RemoteLayer>,
    ) -> anyhow::Result<()> {
        Timeline::download_remote_layer(Arc::clone(&self.timeline), remote_layer).await
    }

    pub fn spawn_ondemand_logical_size_calculation(
        &self,
        lsn: Lsn,
    ) -> oneshot::Receiver<Result<u64, CalculateLogicalSizeError>> {
        Timeline::spawn_ondemand_logical_size_calculation(&self.timeline, lsn)
    }

    async fn wait_to_become_active(&self) -> anyhow::Result<()> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = *receiver.borrow_and_update();
            match current_state {
                TimelineState::Suspended => {
                    // in these states, there's a chance that we can reach ::Active
                    receiver.changed().await?;
                }
                TimelineState::Active { .. } => {
                    return Ok(());
                }
                TimelineState::Broken | TimelineState::Stopping => {
                    // There's no chance the tenant can transition back into ::Active
                    anyhow::bail!(
                        "Timeline {}/{} will not become active. Current state: {:?}",
                        self.timeline.tenant_id,
                        self.timeline.timeline_id,
                        current_state,
                    );
                }
            }
        }
    }
}

impl Deref for TimelineGuard {
    type Target = Timeline;

    fn deref(&self) -> &Self::Target {
        self.timeline.as_ref()
    }
}

impl TimelineRef {
    pub(super) fn new(timeline: &Timeline) -> Self {
        Self {
            timeline: Weak::clone(&timeline.myself),
            id: TenantTimelineId::new(timeline.tenant_id, timeline.timeline_id),
        }
    }

    pub fn any_timeline(&self) -> anyhow::Result<TimelineGuard> {
        // XXX: ensure that the timeline writable now, and that it will remain writable until the guard object is dropped.
        //    : For example, timeline delete should eventually just need wait for all guard objects to be dropped, then proceed
        //    : with timeline state transition & layer file deletion.
        Ok(TimelineGuard {
            timeline: self
                .timeline
                .upgrade()
                .with_context(|| format!("Timeline {} is dropped", self.id))?,
        })
    }

    // TODO kb write some TODO about read/write separation
    pub fn active_timeline(&self) -> anyhow::Result<TimelineGuard> {
        let timeline = self.any_timeline()?;
        anyhow::ensure!(
            timeline.is_active(),
            "Timeline {} is not active, state: {:?}",
            self.id,
            timeline.current_state(),
        );
        Ok(timeline)
    }

    pub async fn get_active_timeline_with_timeout(&self) -> anyhow::Result<TimelineGuard> {
        let timeline = self.any_timeline()?;
        match tokio::time::timeout(Duration::from_secs(30), timeline.wait_to_become_active()).await
        {
            Ok(Ok(())) => Ok(timeline),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("Timeout waiting for timeline {} to become Active", self.id),
        }
    }
}
