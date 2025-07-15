use std::str::FromStr as _;

use crate::worker_process::lfc_metrics::LfcMetricsCollector;

use measured::MetricGroup;
use utils::id::{TenantId, TimelineId};

#[derive(MetricGroup)]
pub struct CommunicatorWorkerProcessStruct {
    /*** Metrics ***/
    #[metric(flatten)]
    pub(crate) lfc_metrics: LfcMetricsCollector,
}

pub(super) async fn init(
    tenant_id: Option<&str>,
    timeline_id: Option<&str>,
) -> CommunicatorWorkerProcessStruct {
    let _tenant_id = tenant_id.map(|s| TenantId::from_str(s).expect("invalid tenant ID"));
    let _timeline_id = timeline_id.map(|s| TimelineId::from_str(s).expect("invalid timeline ID"));

    CommunicatorWorkerProcessStruct {
        // metrics
        lfc_metrics: LfcMetricsCollector::new(),
    }
}
