use std::{
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};

use bytes::Bytes;
use pageserver_api::{reltag::RelTag, shard::TenantShardId};
use utils::lsn::Lsn;

use crate::{config::PageServerConf, walrecord::NeonWalRecord};

mod live_reconfig;
mod no_leak_child;
/// The IPC protocol that pageserver and walredo process speak over their shared pipe.
mod protocol;

mod process_impl {
    pub(super) mod process_async;
    pub(super) mod process_std;
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub(crate) enum Kind {
    Sync,
    Async,
}

impl TryFrom<u8> for Kind {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            v if v == (Kind::Sync as u8) => Kind::Sync,
            v if v == (Kind::Async as u8) => Kind::Async,
            x => return Err(x),
        })
    }
}

static PROCESS_KIND: AtomicU8 = AtomicU8::new(Kind::Async as u8);

pub(crate) fn set_kind(kind: Kind) {
    PROCESS_KIND.store(kind as u8, std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(test))]
    {
        let metric = &crate::metrics::wal_redo::PROCESS_KIND;
        metric.reset();
        metric.with_label_values(&[&format!("{kind:?}")]).set(1);
    }
}

fn get() -> Kind {
    Kind::try_from(PROCESS_KIND.load(Ordering::Relaxed)).unwrap()
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
        Ok(match get() {
            Kind::Sync => Self::Sync(process_impl::process_std::WalRedoProcess::launch(
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
}
