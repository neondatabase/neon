/// Layer of indirection previously used to support multiple implementations.
/// Subject to removal: <https://github.com/neondatabase/neon/issues/7753>
use std::time::Duration;

use bytes::Bytes;
use pageserver_api::{reltag::RelTag, shard::TenantShardId};
use tracing::warn;
use utils::lsn::Lsn;

use crate::{config::PageServerConf, walrecord::NeonWalRecord};

mod no_leak_child;
/// The IPC protocol that pageserver and walredo process speak over their shared pipe.
mod protocol;

mod process_impl {
    pub(super) mod process_async;
}

pub(crate) struct Process(process_impl::process_async::WalRedoProcess);

impl Process {
    #[inline(always)]
    pub fn launch(
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        pg_version: u32,
    ) -> anyhow::Result<Self> {
        if conf.walredo_process_kind != pageserver_api::config::WalRedoProcessKind::Async {
            warn!(
                configured = %conf.walredo_process_kind,
                "the walredo_process_kind setting has been turned into a no-op, using async implementation"
            );
        }
        Ok(Self(process_impl::process_async::WalRedoProcess::launch(
            conf,
            tenant_shard_id,
            pg_version,
        )?))
    }

    #[inline(always)]
    pub(crate) async fn apply_wal_records(
        &self,
        rel: RelTag,
        blknum: u32,
        base_img: &Option<Bytes>,
        records: &[(Lsn, NeonWalRecord)],
        wal_redo_timeout: Duration,
    ) -> anyhow::Result<Bytes> {
        self.0
            .apply_wal_records(rel, blknum, base_img, records, wal_redo_timeout)
            .await
    }

    pub(crate) fn id(&self) -> u32 {
        self.0.id()
    }
}
