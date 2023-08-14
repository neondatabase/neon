//! Newtypes to ensure that dirty buffers are written back to the filesystem before they are dropped.

use std::io::ErrorKind;
use std::ops::Deref;
use std::ops::DerefMut;
use std::os::unix::prelude::FileExt;

use crate::page_cache::PAGE_SZ;

use super::buffer_pool;
use super::EphemeralFile;

pub(super) struct Buffer<'f> {
    inner: Inner<'f>,
}

enum Inner<'f> {
    Dirty {
        ephemeral_file: &'f EphemeralFile,
        buf: buffer_pool::Handle,
        blkno: u32,
    },
    WritebackOngoing,
    WrittenBack,
    WritebackError,
    Dropped,
}

impl<'f> Buffer<'f> {
    pub(super) fn new(
        ephemeral_file: &'f EphemeralFile,
        buf: buffer_pool::Handle,
        blkno: u32,
    ) -> Self {
        Self {
            inner: Inner::Dirty {
                ephemeral_file,
                buf,
                blkno,
            },
        }
    }
    pub(super) fn writeback(mut self) -> Result<(), std::io::Error> {
        let Inner::Dirty {
        ephemeral_file,
        buf,
        blkno,
    } = std::mem::replace(&mut self.inner, Inner::WritebackOngoing) else {
        unreachable!("writeback consumes");
    };
        match ephemeral_file
            .file
            .write_all_at(buf.deref(), blkno as u64 * PAGE_SZ as u64)
        {
            Ok(_) => {
                self.inner = Inner::WrittenBack;
                Ok(())
            }
            Err(e) => {
                self.inner = Inner::WritebackError;
                Err(std::io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "failed to write back to ephemeral file at {} error: {}",
                        ephemeral_file.file.path.display(),
                        e
                    ),
                ))
            }
        }
    }
}

impl<'f> Deref for Buffer<'f> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.inner {
            Inner::Dirty { buf, .. } => &**buf,
            Inner::WritebackOngoing => unreachable!("writeback consumes"),
            Inner::WrittenBack => unreachable!("writeback consumes"),
            Inner::WritebackError => unreachable!("writeback consumes"),
            Inner::Dropped => unreachable!(),
        }
    }
}

impl<'f> DerefMut for Buffer<'f> {
    fn deref_mut(&mut self) -> &mut [u8] {
        match &mut self.inner {
            Inner::Dirty { buf, .. } => &mut **buf,
            Inner::WritebackOngoing => unreachable!("writeback consumes"),
            Inner::WrittenBack => unreachable!("writeback consumes"),
            Inner::WritebackError => unreachable!("writeback consumes"),
            Inner::Dropped => unreachable!(),
        }
    }
}

impl Drop for Buffer<'_> {
    fn drop(&mut self) {
        let prev = std::mem::replace(&mut self.inner, Inner::Dropped);
        match prev {
            // TODO: check this at compile time
            Inner::Dirty { .. } => panic!("dropped dirty buffer, need to writeback() first"),
            Inner::WritebackOngoing => unreachable!("transitory state"),
            Inner::WrittenBack | Inner::WritebackError => {}
            Inner::Dropped => unreachable!("drop only happens once"),
        }
    }
}
