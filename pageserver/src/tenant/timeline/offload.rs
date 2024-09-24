use std::sync::Arc;

use crate::tenant::{OffloadedTimeline, Tenant};

use super::{
    delete::{delete_local_timeline_directory, DeleteTimelineFlow, DeletionGuard},
    Timeline,
};

pub(crate) async fn offload_timeline(
    tenant: &Tenant,
    timeline: &Arc<Timeline>,
) -> anyhow::Result<()> {
    tracing::info!("offloading archived timeline");
    let (timeline, guard) = DeleteTimelineFlow::prepare(tenant, timeline.timeline_id)?;

    // TODO extend guard mechanism above with method
    // to make deletions possible while offloading is in progress

    // TODO mark timeline as offloaded in S3

    let conf = &tenant.conf;
    delete_local_timeline_directory(conf, tenant.tenant_shard_id, &timeline).await?;

    remove_timeline_from_tenant(tenant, &timeline, &guard).await?;

    {
        let mut offloaded_timelines = tenant.timelines_offloaded.lock().unwrap();
        offloaded_timelines.insert(
            timeline.timeline_id,
            Arc::new(OffloadedTimeline::from_timeline(&timeline)),
        );
    }

    Ok(())
}

/// It is important that this gets called when DeletionGuard is being held.
/// For more context see comments in [`DeleteTimelineFlow::prepare`]
async fn remove_timeline_from_tenant(
    tenant: &Tenant,
    timeline: &Timeline,
    _: &DeletionGuard, // using it as a witness
) -> anyhow::Result<()> {
    // Remove the timeline from the map.
    let mut timelines = tenant.timelines.lock().unwrap();
    let children_exist = timelines
        .iter()
        .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline.timeline_id));
    // XXX this can happen because `branch_timeline` doesn't check `TimelineState::Stopping`.
    // We already deleted the layer files, so it's probably best to panic.
    // (Ideally, above remove_dir_all is atomic so we don't see this timeline after a restart)
    if children_exist {
        panic!("Timeline grew children while we removed layer files");
    }

    timelines
        .remove(&timeline.timeline_id)
        .expect("timeline that we were deleting was concurrently removed from 'timelines' map");

    drop(timelines);

    Ok(())
}
