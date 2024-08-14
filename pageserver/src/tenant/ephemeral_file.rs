//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache;
use crate::virtual_file::{self, VirtualFile};
use camino::Utf8PathBuf;
use pageserver_api::shard::TenantShardId;

use std::io;
use std::sync::atomic::AtomicU64;
use utils::id::TimelineId;

pub struct EphemeralFile {
    _tenant_shard_id: TenantShardId,
    _timeline_id: TimelineId,

    rw: page_caching::RW,
}

pub(super) mod page_caching;

use super::storage_layer::inmemory_layer::InMemoryLayerIndexValue;
mod zero_padded_read_write;

impl EphemeralFile {
    pub async fn create(
        conf: &PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: &RequestContext,
    ) -> Result<EphemeralFile, io::Error> {
        static NEXT_FILENAME: AtomicU64 = AtomicU64::new(1);
        let filename_disambiguator =
            NEXT_FILENAME.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let filename = conf
            .timeline_path(&tenant_shard_id, &timeline_id)
            .join(Utf8PathBuf::from(format!(
                "ephemeral-{filename_disambiguator}"
            )));

        let file = VirtualFile::open_with_options(
            &filename,
            virtual_file::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
            ctx,
        )
        .await?;

        Ok(EphemeralFile {
            _tenant_shard_id: tenant_shard_id,
            _timeline_id: timeline_id,
            rw: page_caching::RW::new(file, gate_guard),
        })
    }

    pub(crate) fn len(&self) -> u32 {
        self.rw.bytes_written()
    }

    pub(crate) fn page_cache_file_id(&self) -> page_cache::FileId {
        self.rw.page_cache_file_id()
    }

    /// See [`self::page_caching::RW::load_to_vec`].
    pub(crate) async fn load_to_vec(&self, ctx: &RequestContext) -> Result<Vec<u8>, io::Error> {
        self.rw.load_to_vec(ctx).await
    }

    pub(crate) async fn read_page(
        &self,
        blknum: u32,
        dst: page_caching::PageBuf,
        ctx: &RequestContext,
    ) -> Result<page_caching::ReadResult, io::Error> {
        self.rw.read_page(blknum, dst, ctx).await
    }

    pub(crate) async fn write_blob(
        &mut self,
        buf: &[u8],
        will_init: bool,
        ctx: &RequestContext,
    ) -> Result<InMemoryLayerIndexValue, io::Error> {
        let pos = self.rw.bytes_written();
        let len = u32::try_from(buf.len()).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                anyhow::anyhow!(
                    "EphemeralFile::write_blob value too large: {}: {e}",
                    buf.len()
                ),
            )
        })?;
        pos.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "EphemeralFile::write_blob: overflow",
            )
        })?;

        self.rw.write_all_borrowed(buf, ctx).await?;

        Ok(InMemoryLayerIndexValue {
            pos,
            len,
            will_init,
        })
    }
}

/// Does the given filename look like an ephemeral file?
pub fn is_ephemeral_file(filename: &str) -> bool {
    if let Some(rest) = filename.strip_prefix("ephemeral-") {
        rest.parse::<u32>().is_ok()
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;
    use std::fs;
    use std::str::FromStr;

    fn harness(
        test_name: &str,
    ) -> Result<
        (
            &'static PageServerConf,
            TenantShardId,
            TimelineId,
            RequestContext,
        ),
        io::Error,
    > {
        let repo_dir = PageServerConf::test_repo_dir(test_name);
        let _ = fs::remove_dir_all(&repo_dir);
        let conf = PageServerConf::dummy_conf(repo_dir);
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let tenant_shard_id = TenantShardId::from_str("11000000000000000000000000000000").unwrap();
        let timeline_id = TimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&tenant_shard_id, &timeline_id))?;

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        Ok((conf, tenant_shard_id, timeline_id, ctx))
    }

    #[tokio::test]
    async fn ephemeral_file_holds_gate_open() {
        const FOREVER: std::time::Duration = std::time::Duration::from_secs(5);

        let (conf, tenant_id, timeline_id, ctx) =
            harness("ephemeral_file_holds_gate_open").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let file = EphemeralFile::create(conf, tenant_id, timeline_id, gate.enter().unwrap(), &ctx)
            .await
            .unwrap();

        let mut closing = tokio::task::spawn(async move {
            gate.close().await;
        });

        // gate is entered until the ephemeral file is dropped
        // do not start paused tokio-epoll-uring has a sleep loop
        tokio::time::pause();
        tokio::time::timeout(FOREVER, &mut closing)
            .await
            .expect_err("closing cannot complete before dropping");

        // this is a requirement of the reset_tenant functionality: we have to be able to restart a
        // tenant fast, and for that, we need all tenant_dir operations be guarded by entering a gate
        drop(file);

        tokio::time::timeout(FOREVER, &mut closing)
            .await
            .expect("closing completes right away")
            .expect("closing does not panic");
    }
}
