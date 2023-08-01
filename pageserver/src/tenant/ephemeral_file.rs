//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::page_cache::{self, ReadBufResult, WriteBufResult, PAGE_SZ};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::BlockReader;
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
    file_id: u64,
    _tenant_id: TenantId,
    _timeline_id: TimelineId,
    file: Arc<VirtualFile>,

    pub size: u64,
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
            file_id,
            _tenant_id: tenant_id,
            _timeline_id: timeline_id,
            file: file_rc,
            size: 0,
        })
    }

    fn fill_buffer(&self, buf: &mut [u8], blkno: u32) -> Result<(), io::Error> {
        let mut off = 0;
        while off < PAGE_SZ {
            let n = self
                .file
                .read_at(&mut buf[off..], blkno as u64 * PAGE_SZ as u64 + off as u64)?;

            if n == 0 {
                // Reached EOF. Fill the rest of the buffer with zeros.
                const ZERO_BUF: [u8; PAGE_SZ] = [0u8; PAGE_SZ];

                buf[off..].copy_from_slice(&ZERO_BUF[off..]);
                break;
            }

            off += n;
        }
        Ok(())
    }

    fn get_buf_for_write(&self, blkno: u32) -> Result<page_cache::PageWriteGuard, io::Error> {
        // Look up the right page
        let cache = page_cache::get();
        let mut write_guard = match cache
            .write_ephemeral_buf(self.file_id, blkno)
            .map_err(|e| to_io_error(e, "Failed to write ephemeral buf"))?
        {
            WriteBufResult::Found(guard) => guard,
            WriteBufResult::NotFound(mut guard) => {
                // Read the page from disk into the buffer
                // TODO: if we're overwriting the whole page, no need to read it in first
                self.fill_buffer(guard.deref_mut(), blkno)?;
                guard.mark_valid();

                // And then fall through to modify it.
                guard
            }
        };
        write_guard.mark_dirty();

        Ok(write_guard)
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

impl FileExt for EphemeralFile {
    fn read_at(&self, dstbuf: &mut [u8], offset: u64) -> Result<usize, io::Error> {
        // Look up the right page
        let blkno = (offset / PAGE_SZ as u64) as u32;
        let off = offset as usize % PAGE_SZ;
        let len = min(PAGE_SZ - off, dstbuf.len());

        let read_guard;
        let mut write_guard;

        let cache = page_cache::get();
        let buf = match cache
            .read_ephemeral_buf(self.file_id, blkno)
            .map_err(|e| to_io_error(e, "Failed to read ephemeral buf"))?
        {
            ReadBufResult::Found(guard) => {
                read_guard = guard;
                read_guard.as_ref()
            }
            ReadBufResult::NotFound(guard) => {
                // Read the page from disk into the buffer
                write_guard = guard;
                self.fill_buffer(write_guard.deref_mut(), blkno)?;
                write_guard.mark_valid();

                // And then fall through to read the requested slice from the
                // buffer.
                write_guard.as_ref()
            }
        };

        dstbuf[0..len].copy_from_slice(&buf[off..(off + len)]);
        Ok(len)
    }

    fn write_at(&self, srcbuf: &[u8], offset: u64) -> Result<usize, io::Error> {
        // Look up the right page
        let blkno = (offset / PAGE_SZ as u64) as u32;
        let off = offset as usize % PAGE_SZ;
        let len = min(PAGE_SZ - off, srcbuf.len());

        let mut write_guard;
        let cache = page_cache::get();
        let buf = match cache
            .write_ephemeral_buf(self.file_id, blkno)
            .map_err(|e| to_io_error(e, "Failed to write ephemeral buf"))?
        {
            WriteBufResult::Found(guard) => {
                write_guard = guard;
                write_guard.deref_mut()
            }
            WriteBufResult::NotFound(guard) => {
                // Read the page from disk into the buffer
                // TODO: if we're overwriting the whole page, no need to read it in first
                write_guard = guard;
                self.fill_buffer(write_guard.deref_mut(), blkno)?;
                write_guard.mark_valid();

                // And then fall through to modify it.
                write_guard.deref_mut()
            }
        };

        buf[off..(off + len)].copy_from_slice(&srcbuf[0..len]);
        write_guard.mark_dirty();
        Ok(len)
    }
}

impl BlobWriter for EphemeralFile {
    fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, io::Error> {
        let pos = self.size;

        let mut blknum = (self.size / PAGE_SZ as u64) as u32;
        let mut off = (pos % PAGE_SZ as u64) as usize;

        let mut buf = self.get_buf_for_write(blknum)?;

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
                blknum += 1;
                buf = self.get_buf_for_write(blknum)?;
                buf[0..4 - thislen].copy_from_slice(&len_buf[thislen..]);
                off = 4 - thislen;
            } else {
                buf[off..off + 4].copy_from_slice(&len_buf);
                off += 4;
            }
        }

        // Write the payload
        let mut buf_remain = srcbuf;
        while !buf_remain.is_empty() {
            let mut page_remain = PAGE_SZ - off;
            if page_remain == 0 {
                blknum += 1;
                buf = self.get_buf_for_write(blknum)?;
                off = 0;
                page_remain = PAGE_SZ;
            }
            let this_blk_len = min(page_remain, buf_remain.len());
            buf[off..(off + this_blk_len)].copy_from_slice(&buf_remain[..this_blk_len]);
            off += this_blk_len;
            buf_remain = &buf_remain[this_blk_len..];
        }
        drop(buf);

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
        // drop all pages from page cache
        let cache = page_cache::get();
        cache.drop_buffers_for_ephemeral(self.file_id);

        // remove entry from the hash map
        EPHEMERAL_FILES.write().unwrap().files.remove(&self.file_id);

        // unlink the file
        let res = std::fs::remove_file(&self.file.path);
        if let Err(e) = res {
            warn!(
                "could not remove ephemeral file '{}': {}",
                self.file.path.display(),
                e
            );
        }
    }
}

pub fn writeback(file_id: u64, blkno: u32, buf: &[u8]) -> Result<(), io::Error> {
    if let Some(file) = EPHEMERAL_FILES.read().unwrap().files.get(&file_id) {
        match file.write_all_at(buf, blkno as u64 * PAGE_SZ as u64) {
            Ok(_) => Ok(()),
            Err(e) => Err(io::Error::new(
                ErrorKind::Other,
                format!(
                    "failed to write back to ephemeral file at {} error: {}",
                    file.path.display(),
                    e
                ),
            )),
        }
    } else {
        Err(io::Error::new(
            ErrorKind::Other,
            "could not write back page, not found in ephemeral files hash",
        ))
    }
}

impl BlockReader for EphemeralFile {
    type BlockLease = page_cache::PageReadGuard<'static>;

    fn read_blk(&self, blknum: u32) -> Result<Self::BlockLease, io::Error> {
        // Look up the right page
        let cache = page_cache::get();
        loop {
            match cache
                .read_ephemeral_buf(self.file_id, blknum)
                .map_err(|e| to_io_error(e, "Failed to read ephemeral buf"))?
            {
                ReadBufResult::Found(guard) => return Ok(guard),
                ReadBufResult::NotFound(mut write_guard) => {
                    // Read the page from disk into the buffer
                    self.fill_buffer(write_guard.deref_mut(), blknum)?;
                    write_guard.mark_valid();

                    // Swap for read lock
                    continue;
                }
            };
        }
    }
}

fn to_io_error(e: anyhow::Error, context: &str) -> io::Error {
    io::Error::new(ErrorKind::Other, format!("{context}: {e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::blob_io::BlobWriter;
    use crate::tenant::block_io::BlockCursor;
    use rand::{seq::SliceRandom, thread_rng, RngCore};
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

    // Helper function to slurp contents of a file, starting at the current position,
    // into a string
    fn read_string(efile: &EphemeralFile, offset: u64, len: usize) -> Result<String, io::Error> {
        let mut buf = Vec::new();
        buf.resize(len, 0u8);

        efile.read_exact_at(&mut buf, offset)?;

        Ok(String::from_utf8_lossy(&buf)
            .trim_end_matches('\0')
            .to_string())
    }

    #[test]
    fn test_ephemeral_files() -> Result<(), io::Error> {
        let (conf, tenant_id, timeline_id) = harness("ephemeral_files")?;

        let file_a = EphemeralFile::create(conf, tenant_id, timeline_id)?;

        file_a.write_all_at(b"foo", 0)?;
        assert_eq!("foo", read_string(&file_a, 0, 20)?);

        file_a.write_all_at(b"bar", 3)?;
        assert_eq!("foobar", read_string(&file_a, 0, 20)?);

        // Open a lot of files, enough to cause some page evictions.
        let mut efiles = Vec::new();
        for fileno in 0..100 {
            let efile = EphemeralFile::create(conf, tenant_id, timeline_id)?;
            efile.write_all_at(format!("file {}", fileno).as_bytes(), 0)?;
            assert_eq!(format!("file {}", fileno), read_string(&efile, 0, 10)?);
            efiles.push((fileno, efile));
        }

        // Check that all the files can still be read from. Use them in random order for
        // good measure.
        efiles.as_mut_slice().shuffle(&mut thread_rng());
        for (fileno, efile) in efiles.iter_mut() {
            assert_eq!(format!("file {}", fileno), read_string(efile, 0, 10)?);
        }

        Ok(())
    }

    #[test]
    fn test_ephemeral_blobs() -> Result<(), io::Error> {
        let (conf, tenant_id, timeline_id) = harness("ephemeral_blobs")?;

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id)?;

        let pos_foo = file.write_blob(b"foo")?;
        assert_eq!(b"foo", file.block_cursor().read_blob(pos_foo)?.as_slice());
        let pos_bar = file.write_blob(b"bar")?;
        assert_eq!(b"foo", file.block_cursor().read_blob(pos_foo)?.as_slice());
        assert_eq!(b"bar", file.block_cursor().read_blob(pos_bar)?.as_slice());

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
            let actual = cursor.read_blob(pos)?;
            assert_eq!(actual, expected);
        }

        // Test a large blob that spans multiple pages
        let mut large_data = Vec::new();
        large_data.resize(20000, 0);
        thread_rng().fill_bytes(&mut large_data);
        let pos_large = file.write_blob(&large_data)?;
        let result = file.block_cursor().read_blob(pos_large)?;
        assert_eq!(result, large_data);

        Ok(())
    }
}
