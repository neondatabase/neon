use safekeeper_api::models::{
    self, PullTimelineRequest, PullTimelineResponse, SafekeeperUtilization, TimelineCreateRequest,
};
use safekeeper_client::mgmt_api::{Client, Result};
use utils::id::{NodeId, TenantId, TimelineId};
use utils::logging::SecretString;

use crate::metrics::PageserverRequestLabelGroup;

/// Thin wrapper around [`safekeeper_client::mgmt_api::Client`]. It allows the storage
/// controller to collect metrics in a non-intrusive manner.
///
/// Analogous to [`crate::pageserver_client::PageserverClient`].
#[derive(Debug, Clone)]
pub(crate) struct SafekeeperClient {
    inner: Client,
    node_id_label: String,
}

macro_rules! measured_request {
    ($name:literal, $method:expr, $node_id: expr, $invoke:expr) => {{
        let labels = PageserverRequestLabelGroup {
            pageserver_id: $node_id,
            path: $name,
            method: $method,
        };

        let latency = &crate::metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_safekeeper_request_latency;
        let _timer_guard = latency.start_timer(labels.clone());

        let res = $invoke;

        if res.is_err() {
            let error_counters = &crate::metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_pageserver_request_error;
            error_counters.inc(labels)
        }

        res
    }};
}

impl SafekeeperClient {
    pub(crate) fn new(
        node_id: NodeId,
        raw_client: reqwest::Client,
        mgmt_api_endpoint: String,
        jwt: Option<SecretString>,
    ) -> Self {
        Self {
            inner: Client::new(raw_client, mgmt_api_endpoint, jwt),
            node_id_label: node_id.0.to_string(),
        }
    }

    pub(crate) async fn create_timeline(
        &self,
        req: &TimelineCreateRequest,
    ) -> Result<reqwest::Response> {
        measured_request!(
            "create_timeline",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.create_timeline(req).await
        )
    }

    #[allow(unused)]
    pub(crate) async fn exclude_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &models::TimelineMembershipSwitchRequest,
    ) -> Result<models::TimelineDeleteResult> {
        measured_request!(
            "exclude_timeline",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner
                .exclude_timeline(tenant_id, timeline_id, req)
                .await
        )
    }

    pub(crate) async fn delete_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<models::TimelineDeleteResult> {
        measured_request!(
            "delete_timeline",
            crate::metrics::Method::Delete,
            &self.node_id_label,
            self.inner.delete_timeline(tenant_id, timeline_id).await
        )
    }

    #[allow(unused)]
    pub(crate) async fn switch_timeline_membership(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &models::TimelineMembershipSwitchRequest,
    ) -> Result<models::TimelineMembershipSwitchResponse> {
        measured_request!(
            "switch_timeline_membership",
            crate::metrics::Method::Put,
            &self.node_id_label,
            self.inner
                .switch_timeline_membership(tenant_id, timeline_id, req)
                .await
        )
    }

    pub(crate) async fn delete_tenant(
        &self,
        tenant_id: TenantId,
    ) -> Result<models::TenantDeleteResult> {
        measured_request!(
            "delete_tenant",
            crate::metrics::Method::Delete,
            &self.node_id_label,
            self.inner.delete_tenant(tenant_id).await
        )
    }

    pub(crate) async fn pull_timeline(
        &self,
        req: &PullTimelineRequest,
    ) -> Result<PullTimelineResponse> {
        measured_request!(
            "pull_timeline",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.pull_timeline(req).await
        )
    }

    #[allow(unused)]
    pub(crate) async fn bump_timeline_term(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &models::TimelineTermBumpRequest,
    ) -> Result<models::TimelineTermBumpResponse> {
        measured_request!(
            "term_bump",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner
                .bump_timeline_term(tenant_id, timeline_id, req)
                .await
        )
    }

    pub(crate) async fn get_utilization(&self) -> Result<SafekeeperUtilization> {
        measured_request!(
            "utilization",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.utilization().await
        )
    }
}
