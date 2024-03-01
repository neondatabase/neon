use super::super::index::LayerFileMetadata;
use crate::config::PageServerConf;
use crate::tenant::storage_layer::LayerFileName;
use pageserver_api::shard::TenantShardId;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use tokio_util;
use tokio_util::sync::CancellationToken;
use utils::id::TimelineId;

mod legacy;
mod tokio_epoll_uring;

pub async fn download_layer_file<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    layer_file_name: &'a LayerFileName,
    layer_metadata: &'a LayerFileMetadata,
    cancel: &CancellationToken,
) -> Result<u64, DownloadError> {
    match crate::virtual_file::io_engine::get() {
        crate::virtual_file::io_engine::IoEngine::NotSet => panic!("unset"),
        crate::virtual_file::io_engine::IoEngine::StdFs => {
            legacy::download_layer_file(
                conf,
                storage,
                tenant_shard_id,
                timeline_id,
                layer_file_name,
                layer_metadata,
                cancel,
            )
            .await
        }
        #[cfg(target_os = "linux")]
        crate::virtual_file::io_engine::IoEngine::TokioEpollUring => {
            tokio_epoll_uring::download_layer_file(
                conf,
                storage,
                tenant_shard_id,
                timeline_id,
                layer_file_name,
                layer_metadata,
                cancel,
            )
            .await
        }
    }
}
