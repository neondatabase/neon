//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::page_cache;
use crate::page_cache::PAGE_SZ;
use crate::page_cache::{ReadBufResult, WriteBufResult};
use crate::virtual_file::VirtualFile;
use lazy_static::lazy_static;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Seek, SeekFrom, Write};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use zenith_utils::zid::ZTenantId;
use zenith_utils::zid::ZTimelineId;

use std::os::unix::fs::FileExt;

lazy_static! {
    ///
    /// This is the global cache of file descriptors (File objects).
    ///
    static ref EPHEMERAL_FILES: RwLock<EphemeralFiles> = RwLock::new(EphemeralFiles {
        next_file_id: 1,
        files: HashMap::new(),
    });
}

pub struct EphemeralFiles {
    next_file_id: u64,

    files: HashMap<u64, Arc<VirtualFile>>,
}

pub struct EphemeralFile {
    file_id: u64,
    _tenantid: ZTenantId,
    _timelineid: ZTimelineId,
    file: Arc<VirtualFile>,

    pos: u64,
}

impl EphemeralFile {
    pub fn create(
        conf: &PageServerConf,
        tenantid: ZTenantId,
        timelineid: ZTimelineId,
    ) -> Result<EphemeralFile, std::io::Error> {
        let mut l = EPHEMERAL_FILES.write().unwrap();
        let file_id = l.next_file_id;
        l.next_file_id += 1;

        let filename = conf
            .timeline_path(&timelineid, &tenantid)
            .join(PathBuf::from(format!("ephemeral-{}", file_id)));

        let file = VirtualFile::open_with_options(
            &filename,
            OpenOptions::new().read(true).write(true).create(true),
        )?;
        let file_rc = Arc::new(file);
        l.files.insert(file_id, file_rc.clone());

        Ok(EphemeralFile {
            file_id,
            _tenantid: tenantid,
            _timelineid: timelineid,
            file: file_rc,
            pos: 0,
        })
    }

    pub fn fill_buffer(&self, buf: &mut [u8], blkno: u32) -> Result<(), Error> {
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

            off += n as usize;
        }
        Ok(())
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
    fn read_at(&self, dstbuf: &mut [u8], offset: u64) -> Result<usize, Error> {
        // Look up the right page
        let blkno = (offset / PAGE_SZ as u64) as u32;
        let off = offset as usize % PAGE_SZ;
        let len = min(PAGE_SZ - off, dstbuf.len());

        let read_guard;
        let mut write_guard;

        let cache = page_cache::get();
        let buf = match cache.read_ephemeral_buf(self.file_id, blkno) {
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

    fn write_at(&self, srcbuf: &[u8], offset: u64) -> Result<usize, Error> {
        // Look up the right page
        let blkno = (offset / PAGE_SZ as u64) as u32;
        let off = offset as usize % PAGE_SZ;
        let len = min(PAGE_SZ - off, srcbuf.len());

        let mut write_guard;
        let cache = page_cache::get();
        let buf = match cache.write_ephemeral_buf(self.file_id, blkno) {
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

impl Write for EphemeralFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let n = self.write_at(buf, self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        todo!()
    }
}

impl Seek for EphemeralFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        match pos {
            SeekFrom::Start(offset) => {
                self.pos = offset;
            }
            SeekFrom::End(_offset) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "SeekFrom::End not supported by EphemeralFile",
                ));
            }
            SeekFrom::Current(offset) => {
                let pos = self.pos as i128 + offset as i128;
                if pos < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "offset would be negative",
                    ));
                }
                if pos > u64::MAX as i128 {
                    return Err(Error::new(ErrorKind::InvalidInput, "offset overflow"));
                }
                self.pos = pos as u64;
            }
        }
        Ok(self.pos)
    }
}

impl Drop for EphemeralFile {
    fn drop(&mut self) {
        // drop all pages from page cache
        let cache = page_cache::get();
        cache.drop_buffers_for_ephemeral(self.file_id);

        // remove entry from the hash map
        EPHEMERAL_FILES.write().unwrap().files.remove(&self.file_id);

        // unlink file
        // FIXME: print error
        let _ = std::fs::remove_file(&self.file.path);
    }
}

pub fn writeback(file_id: u64, blkno: u32, buf: &[u8]) -> Result<(), std::io::Error> {
    if let Some(file) = EPHEMERAL_FILES.read().unwrap().files.get(&file_id) {
        file.write_all_at(buf, blkno as u64 * PAGE_SZ as u64)?;
        Ok(())
    } else {
        Err(std::io::Error::new(
            ErrorKind::Other,
            "could not write back page, not found in ephemeral files hash",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use std::fs;
    use std::str::FromStr;

    fn repo_harness(
        test_name: &str,
    ) -> Result<(&'static PageServerConf, ZTenantId, ZTimelineId), Error> {
        let repo_dir = PageServerConf::test_repo_dir(test_name);
        let _ = fs::remove_dir_all(&repo_dir);
        let conf = PageServerConf::dummy_conf(repo_dir);
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let tenantid = ZTenantId::from_str("11000000000000000000000000000000").unwrap();
        let timelineid = ZTimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&timelineid, &tenantid))?;

        Ok((conf, tenantid, timelineid))
    }

    // Helper function to slurp contents of a file, starting at the current position,
    // into a string
    fn read_string(efile: &EphemeralFile, offset: u64, len: usize) -> Result<String, Error> {
        let mut buf = Vec::new();
        buf.resize(len, 0u8);

        efile.read_exact_at(&mut buf, offset)?;

        Ok(String::from_utf8_lossy(&buf)
            .trim_end_matches('\0')
            .to_string())
    }

    #[test]
    fn test_ephemeral_files() -> Result<(), Error> {
        let (conf, tenantid, timelineid) = repo_harness("ephemeral_files")?;

        let mut file_a = EphemeralFile::create(conf, tenantid, timelineid)?;

        file_a.write_all(b"foo")?;
        assert_eq!("foo", read_string(&file_a, 0, 20)?);

        file_a.write_all(b"bar")?;
        assert_eq!("foobar", read_string(&file_a, 0, 20)?);

        // Open a lot of files, enough to cause some page evictions.
        let mut efiles = Vec::new();
        for fileno in 0..100 {
            let mut efile = EphemeralFile::create(conf, tenantid, timelineid)?;
            efile.write_all(format!("file {}", fileno).as_bytes())?;
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
}
