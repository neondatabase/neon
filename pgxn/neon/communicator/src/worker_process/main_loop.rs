use std::str::FromStr as _;

use crate::worker_process::lfc_metrics::LfcMetricsCollector;

use utils::id::{TenantId, TimelineId};

pub struct CommunicatorWorkerProcessStruct {
    /*** Metrics ***/
    pub(crate) lfc_metrics: LfcMetricsCollector,
}

pub(super) async fn init(
    tenant_id: String,
    timeline_id: String,
) -> CommunicatorWorkerProcessStruct {
    let _tenant_id = TenantId::from_str(&tenant_id).expect("invalid tenant ID");
    let _timeline_id = TimelineId::from_str(&timeline_id).expect("invalid timeline ID");

    CommunicatorWorkerProcessStruct {
        // metrics
        lfc_metrics: LfcMetricsCollector::new(),
    }
}

impl metrics::core::Collector for CommunicatorWorkerProcessStruct {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();

        descs.append(&mut self.lfc_metrics.desc());

        descs
    }
    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        let mut values = Vec::new();

        values.append(&mut self.lfc_metrics.collect());

        values
    }
}
