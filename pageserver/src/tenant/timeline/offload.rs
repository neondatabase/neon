use std::sync::Arc;

use crate::tenant::Tenant;

use super::{
    delete::{delete_local_timeline_directory, remove_timeline_from_tenant, DeleteTimelineFlow},
    Timeline,
};

pub(crate) async fn offload_timeline(
    tenant: &Tenant,
    timeline: &Arc<Timeline>,
) -> anyhow::Result<()> {
    let (timeline, guard) = DeleteTimelineFlow::prepare(tenant, timeline.timeline_id)?;

    // TODO extend guard mechanism above with method
    // to make deletions possible while offloading is in progress

    // TODO mark timeline as offloaded in S3

    let conf = &tenant.conf;
    delete_local_timeline_directory(conf, tenant.tenant_shard_id, &timeline).await?;

    remove_timeline_from_tenant(tenant, &timeline, &guard).await?;
    Ok(())
}
