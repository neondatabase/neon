use std::time::Duration;

use bytes::Bytes;
use pageserver_api::{reltag::RelTag, shard::TenantShardId};
use utils::lsn::Lsn;

use crate::{config::PageServerConf, walrecord::NeonWalRecord};

mod no_leak_child;
/// The IPC protocol that pageserver and walredo process speak over their shared pipe.
mod protocol;

mod process_impl {
    pub(super) mod process_async;
    pub(super) mod process_std;
}



pub(crate) enum process {
    sync(process_impl::process_std::walredoprocess),
    async(process_impl::process_async::walredoprocess),
}

impl process {
    #[inline(always)]
    pub fn launch(
        conf: &'static pageserverconf,
        tenant_shard_id: tenantshardid,
        pg_version: u32,
    ) -> anyhow::result<self> {
        ok(match conf.walredo_process_kind {
            kind::sync => self::sync(process_impl::process_std::walredoprocess::launch(
                conf,
                tenant_shard_id,
                pg_version,
            )?),
            Kind::Async => Self::Async(process_impl::process_async::WalRedoProcess::launch(
                conf,
                tenant_shard_id,
                pg_version,
            )?),
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

    pub(crate) fn kind(&self) -> Kind {
        match self {
            Process::Sync(_) => Kind::Sync,
            Process::Async(_) => Kind::Async,
        }
    }
}
