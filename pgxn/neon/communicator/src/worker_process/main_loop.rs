use std::str::FromStr as _;

use crate::worker_process::lfc_metrics::LfcMetricsCollector;

use measured::MetricGroup;
use measured::metric::MetricEncoding;
use measured::metric::gauge::GaugeState;
use measured::metric::group::Encoding;
use utils::id::{TenantId, TimelineId};

pub struct CommunicatorWorkerProcessStruct {
    runtime: tokio::runtime::Runtime,

    /*** Metrics ***/
    pub(crate) lfc_metrics: LfcMetricsCollector,
}

/// Launch the communicator process's Rust subsystems
pub(super) fn init(
    tenant_id: Option<&str>,
    timeline_id: Option<&str>,
) -> Result<&'static CommunicatorWorkerProcessStruct, String> {
    let _tenant_id = tenant_id.map(|s| TenantId::from_str(s).expect("invalid tenant ID"));
    let _timeline_id = timeline_id.map(|s| TimelineId::from_str(s).expect("invalid timeline ID"));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("communicator thread")
        .build()
        .unwrap();

    let worker_struct = CommunicatorWorkerProcessStruct {
        runtime,

        // metrics
        lfc_metrics: LfcMetricsCollector::new(),
    };
    let worker_struct = Box::leak(Box::new(worker_struct));

    worker_struct
        .runtime
        .block_on(worker_struct.launch_metrics_exporter())
        .map_err(|e| e.to_string())?;

    Ok(worker_struct)
}

impl<T> MetricGroup<T> for CommunicatorWorkerProcessStruct
where
    T: Encoding,
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), T::Err> {
        self.lfc_metrics.collect_group_into(enc)
    }
}
