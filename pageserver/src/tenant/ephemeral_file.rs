//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockLease, BlockReader};
use crate::virtual_file::VirtualFile;
use once_cell::sync::Lazy;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, ErrorKind};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::*;
use utils::id::{TenantId, TimelineId};

use std::os::unix::fs::FileExt;

///
/// This is the global cache of file descriptors (File objects).
///
static EPHEMERAL_FILES: Lazy<RwLock<EphemeralFiles>> = Lazy::new(|| {
    RwLock::new(EphemeralFiles {
        next_file_id: 1,
        files: HashMap::new(),
    })
});

pub struct EphemeralFiles {
    next_file_id: u64,

    files: HashMap<u64, Arc<VirtualFile>>,
}

pub struct EphemeralFile {
    ephemeral_files_id: u64,
    page_cache_file_id: page_cache::FileId,

    _tenant_id: TenantId,
    _timeline_id: TimelineId,
    file: Arc<VirtualFile>,
    size: u64,

    mutable_head: [u8; PAGE_SZ],
}

impl EphemeralFile {
    pub fn create(
        conf: &PageServerConf,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<EphemeralFile, io::Error> {
        let mut l = EPHEMERAL_FILES.write().unwrap();
        let file_id = l.next_file_id;
        l.next_file_id += 1;

        let filename = conf
            .timeline_path(&tenant_id, &timeline_id)
            .join(PathBuf::from(format!("ephemeral-{}", file_id)));

        let file = VirtualFile::open_with_options(
            &filename,
            OpenOptions::new().read(true).write(true).create(true),
        )?;
        let file_rc = Arc::new(file);
        l.files.insert(file_id, file_rc.clone());

        Ok(EphemeralFile {
            ephemeral_files_id: file_id,
            page_cache_file_id: page_cache::next_file_id(),
            _tenant_id: tenant_id,
            _timeline_id: timeline_id,
            file: file_rc,
            size: 0,
            mutable_head: [0u8; PAGE_SZ],
        })
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
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

impl BlobWriter for EphemeralFile {
    fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, io::Error> {
        let pos = self.size;

        // TODO: rewrite this to use bytes::BytesMut / a wrapper that
        // auto-flushes underneath. It'd be much less error-prone.

        let mut blknum = (self.size / PAGE_SZ as u64) as u32;
        let mut off = (pos % PAGE_SZ as u64) as usize;

        let buf = &mut self.mutable_head;
        let flush_head = |head: &mut [u8], blknum: u32| {
            debug_assert_eq!(head.len(), PAGE_SZ);
            match self.file.write_all_at(head, blknum as u64 * PAGE_SZ as u64) {
                Ok(_) => {
                    // Pre-warm the page cache with what we just wrote.
                    // This isn't necessary for coherency/correctness, but it's how we've always done it.
                    let cache = page_cache::get();
                    match cache.read_immutable_buf(self.page_cache_file_id, blknum) {
                        Err(e) => {
                            error!("ephemeral_file flush_head failed to get immutable buf to pre-warm page cache: {e:?}");
                            // fail gracefully, it's not the end of the world if we can't pre-warm the cache here
                        }
                        Ok(page_cache::ReadBufResult::Found(_guard)) => {
                            // This function takes &mut self, so, it shouldn't be possible to reach this point.
                            unreachable!("we just wrote blknum {blknum} and this function takes &mut self, so, no concurrent read_blk is possible");
                        }
                        Ok(page_cache::ReadBufResult::NotFound(mut write_guard)) => {
                            let buf: &mut [u8] = write_guard.deref_mut();
                            debug_assert_eq!(buf.len(), PAGE_SZ);
                            buf.copy_from_slice(head);
                            write_guard.mark_valid();
                            // pre-warm successful
                        }
                    }
                    // Zero the buffer for re-use.
                    // Zeroing is critical for correcntess because the write_blob code below
                    // and similarly read_blk expect zeroed pages.
                    const ZERO: [u8; PAGE_SZ] = [0u8; PAGE_SZ];
                    head.copy_from_slice(&ZERO);
                    Ok(())
                }
                Err(e) => Err(std::io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "failed to write back to ephemeral file at {} error: {}",
                        self.file.path.display(),
                        e
                    ),
                )),
            }
        };

        // Write the length field
        if srcbuf.len() < 0x80 {
            buf[off] = srcbuf.len() as u8;
            off += 1;
        } else {
            let mut len_buf = u32::to_be_bytes(srcbuf.len() as u32);
            len_buf[0] |= 0x80;
            let thislen = PAGE_SZ - off;
            if thislen < 4 {
                // it needs to be split across pages
                buf[off..(off + thislen)].copy_from_slice(&len_buf[..thislen]);
                flush_head(buf, blknum)?;
                blknum += 1;
                buf[0..4 - thislen].copy_from_slice(&len_buf[thislen..]);
                off = 4 - thislen;
            } else {
                buf[off..off + 4].copy_from_slice(&len_buf);
                off += 4;
            }
        }

        // Write the payload
        let mut buf_remain = srcbuf;
        loop {
            let page_remain = PAGE_SZ - off;
            if page_remain == 0 {
                flush_head(buf, blknum)?;
                blknum += 1;
                off = 0;
                continue;
            }
            if buf_remain.is_empty() {
                break;
            }
            let this_blk_len = min(page_remain, buf_remain.len());
            buf[off..(off + this_blk_len)].copy_from_slice(&buf_remain[..this_blk_len]);
            off += this_blk_len;
            buf_remain = &buf_remain[this_blk_len..];
        }

        if srcbuf.len() < 0x80 {
            self.size += 1;
        } else {
            self.size += 4;
        }
        self.size += srcbuf.len() as u64;

        Ok(pos)
    }
}

impl Drop for EphemeralFile {
    fn drop(&mut self) {
        // remove entry from the hash map
        EPHEMERAL_FILES
            .write()
            .unwrap()
            .files
            .remove(&self.ephemeral_files_id);

        // unlink the file
        let res = std::fs::remove_file(&self.file.path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!(
                    "could not remove ephemeral file '{}': {}",
                    self.file.path.display(),
                    e
                );
            }
        }
    }
}

impl BlockReader for EphemeralFile {
    fn read_blk(&self, blknum: u32) -> Result<BlockLease, io::Error> {
        let flushed_blknums = 0..self.size / PAGE_SZ as u64;
        if flushed_blknums.contains(&(blknum as u64)) {
            let cache = page_cache::get();
            loop {
                match cache
                    .read_immutable_buf(self.page_cache_file_id, blknum)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to read immutable buf: {e:#}"),
                        )
                    })? {
                    page_cache::ReadBufResult::Found(guard) => {
                        return Ok(BlockLease::PageReadGuard(guard))
                    }
                    page_cache::ReadBufResult::NotFound(mut write_guard) => {
                        let buf: &mut [u8] = write_guard.deref_mut();
                        debug_assert_eq!(buf.len(), PAGE_SZ);
                        self.file
                            .read_exact_at(&mut buf[..], blknum as u64 * PAGE_SZ as u64)?;
                        write_guard.mark_valid();
                        // Swap for read lock
                        continue;
                    }
                };
            }
        } else {
            debug_assert_eq!(blknum as u64, self.size / PAGE_SZ as u64);
            Ok(BlockLease::EphemeralFileMutableHead(&self.mutable_head))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::blob_io::BlobWriter;
    use crate::tenant::block_io::BlockCursor;
    use rand::{thread_rng, RngCore};
    use std::fs;
    use std::str::FromStr;

    fn harness(
        test_name: &str,
    ) -> Result<(&'static PageServerConf, TenantId, TimelineId), io::Error> {
        let repo_dir = PageServerConf::test_repo_dir(test_name);
        let _ = fs::remove_dir_all(&repo_dir);
        let conf = PageServerConf::dummy_conf(repo_dir);
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let tenant_id = TenantId::from_str("11000000000000000000000000000000").unwrap();
        let timeline_id = TimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&tenant_id, &timeline_id))?;

        Ok((conf, tenant_id, timeline_id))
    }

    #[tokio::test]
    async fn test_ephemeral_blobs() -> Result<(), io::Error> {
        let (conf, tenant_id, timeline_id) = harness("ephemeral_blobs")?;

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id)?;

        let pos_foo = file.write_blob(b"foo")?;
        assert_eq!(
            b"foo",
            file.block_cursor().read_blob(pos_foo).await?.as_slice()
        );
        let pos_bar = file.write_blob(b"bar")?;
        assert_eq!(
            b"foo",
            file.block_cursor().read_blob(pos_foo).await?.as_slice()
        );
        assert_eq!(
            b"bar",
            file.block_cursor().read_blob(pos_bar).await?.as_slice()
        );

        let mut blobs = Vec::new();
        for i in 0..10000 {
            let data = Vec::from(format!("blob{}", i).as_bytes());
            let pos = file.write_blob(&data)?;
            blobs.push((pos, data));
        }
        // also test with a large blobs
        for i in 0..100 {
            let data = format!("blob{}", i).as_bytes().repeat(100);
            let pos = file.write_blob(&data)?;
            blobs.push((pos, data));
        }

        let cursor = BlockCursor::new(&file);
        for (pos, expected) in blobs {
            let actual = cursor.read_blob(pos).await?;
            assert_eq!(actual, expected);
        }

        // Test a large blob that spans multiple pages
        let mut large_data = Vec::new();
        large_data.resize(20000, 0);
        thread_rng().fill_bytes(&mut large_data);
        let pos_large = file.write_blob(&large_data)?;
        let result = file.block_cursor().read_blob(pos_large).await?;
        assert_eq!(result, large_data);

        Ok(())
    }
}
