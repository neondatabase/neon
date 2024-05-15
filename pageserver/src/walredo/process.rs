use std::time::Duration;

use bytes::Bytes;
use pageserver_api::{config::WalRedoProcessKind, reltag::RelTag, shard::TenantShardId};
use utils::lsn::Lsn;

use crate::{config::PageServerConf, walrecord::NeonWalRecord};

mod no_leak_child;
/// The IPC protocol that pageserver and walredo process speak over their shared pipe.
mod protocol;

mod process_impl {
    pub(super) mod process_async;
    pub(super) mod process_std;
}

pub(crate) enum Process {
    Sync(process_impl::process_std::WalRedoProcess),
    Async(process_impl::process_async::WalRedoProcess),
}

impl Process {
    #[inline(always)]
    pub fn launch(
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        pg_version: u32,
    ) -> anyhow::Result<Self> {
        Ok(match conf.walredo_process_kind {
            pageserver_api::config::WalRedoProcessKind::Sync => {
                Self::Sync(process_impl::process_std::WalRedoProcess::launch(
                    conf,
                    tenant_shard_id,
                    pg_version,
                )?)
            }
            pageserver_api::config::WalRedoProcessKind::Async => {
                Self::Async(process_impl::process_async::WalRedoProcess::launch(
                    conf,
                    tenant_shard_id,
                    pg_version,
                )?)
            }
        })
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
        match self {
            Process::Sync(p) => {
                p.apply_wal_records(rel, blknum, base_img, records, wal_redo_timeout)
                    .await
            }
            Process::Async(p) => {
                p.apply_wal_records(rel, blknum, base_img, records, wal_redo_timeout)
                    .await
            }
        }
    }

    pub(crate) fn id(&self) -> u32 {
        match self {
            Process::Sync(p) => p.id(),
            Process::Async(p) => p.id(),
        }
    }

    pub(crate) fn kind(&self) -> WalRedoProcessKind {
        match self {
            Process::Sync(_) => WalRedoProcessKind::Sync,
            Process::Async(_) => WalRedoProcessKind::Async,
        }
    }
}
