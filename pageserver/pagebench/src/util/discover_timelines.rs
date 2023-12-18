use std::sync::Arc;

use pageserver_client::mgmt_api;
use tokio::task::JoinSet;
use utils::id::TenantId;

use super::tenant_timeline_id::TenantTimelineId;

pub(crate) async fn get_pageserver_tenant_timelines(
    api_client: &Arc<mgmt_api::Client>,
) -> anyhow::Result<Vec<TenantTimelineId>> {
    let mut timelines: Vec<TenantTimelineId> = Vec::new();
    let mut tenants: Vec<TenantId> = Vec::new();
    for ti in api_client.list_tenants().await? {
        if !ti.id.is_unsharded() {
            anyhow::bail!(
                "only unsharded tenants are supported at this time: {}",
                ti.id
            );
        }
        tenants.push(ti.id.tenant_id)
    }
    let mut js = JoinSet::new();
    for tenant_id in tenants {
        js.spawn({
            let mgmt_api_client = Arc::clone(api_client);
            async move {
                (
                    tenant_id,
                    mgmt_api_client.tenant_details(tenant_id).await.unwrap(),
                )
            }
        });
    }
    while let Some(res) = js.join_next().await {
        let (tenant_id, details) = res.unwrap();
        for timeline_id in details.timelines {
            timelines.push(TenantTimelineId {
                tenant_id,
                timeline_id,
            });
        }
    }
    Ok(timelines)
}
