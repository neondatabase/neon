use std::{
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};

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

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(serialize_all = "kebab-case")]
#[repr(u8)]
pub enum Kind {
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

impl Kind {
    pub const DEFAULT: Kind = Kind::Sync;
    pub const DEFAULT_TOML: &'static str = "sync";
}

static PROCESS_KIND: AtomicU8 = AtomicU8::new(Kind::DEFAULT as u8);

/// Walredo processes that are [`Process::launch`]ed after the call to this function returns
/// will be of the given `kind`. Existing processes are not affected.
pub fn set_kind(kind: Kind) {
    PROCESS_KIND.store(kind as u8, std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(test))]
    crate::metrics::wal_redo::sync_metric_with_runtime_value();
}

pub fn get_kind() -> Kind {
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
        Ok(match get_kind() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_defaults_are_eq() {
        #[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
        struct Doc {
            val: Kind,
        }
        let de = Doc { val: Kind::DEFAULT };
        let default_toml = Kind::DEFAULT_TOML;
        let ser = format!(
            r#"
        val = '{default_toml}'
        "#
        );
        assert_eq!(de, toml_edit::de::from_str(&ser).unwrap(),);
    }
}
