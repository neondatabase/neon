use std::sync::Arc;

use pageserver_client::mgmt_api;
use tracing::info;
use utils::id::TenantTimelineId;

pub(crate) struct Spec {
    pub(crate) limit_to_first_n_targets: Option<usize>,
    pub(crate) targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) async fn discover(
    api_client: &Arc<mgmt_api::Client>,
    spec: Spec,
) -> anyhow::Result<Vec<TenantTimelineId>> {
    let mut timelines = if let Some(targets) = spec.targets {
        targets
    } else {
        mgmt_api::util::get_pageserver_tenant_timelines_unsharded(api_client).await?
    };

    if let Some(limit) = spec.limit_to_first_n_targets {
        timelines.sort(); // for determinism
        timelines.truncate(limit);
        if timelines.len() < limit {
            anyhow::bail!("pageserver has less than limit_to_first_n_targets={limit} tenants");
        }
    }

    info!("timelines:\n{:?}", timelines);
    info!("number of timelines:\n{:?}", timelines.len());

    Ok(timelines)
}
