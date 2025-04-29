//! Implement the "low-level" parts of the file cache.
//!
//! This module just deals with reading and writing the file, and keeping track
//! which blocks in the cache file are in use and which are free. The "high
//! level" parts of tracking which block in the cache file corresponds to which
//! relation block is handled in 'integrated_cache' instead.
//!
//! This module is only used to access the file from the communicator
//! process. The backend processes *also* read the file (and sometimes also
//! write it? ), but the backends use direct C library calls for that.
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio_epoll_uring;

use crate::BLCKSZ;

pub type CacheBlock = u64;

pub struct FileCache {
    uring_system: tokio_epoll_uring::SystemHandle,

    file: Arc<File>,

    // TODO: there's no reclamation mechanism, the cache grows
    // indefinitely. This is the next free block, i.e. the current
    // size of the file
    next_free_block: AtomicU64,
}

impl FileCache {
    pub fn new(
        file_cache_path: &Path,
        uring_system: tokio_epoll_uring::SystemHandle,
    ) -> Result<FileCache, std::io::Error> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(file_cache_path)?;

        tracing::info!("Created cache file {file_cache_path:?}");

        Ok(FileCache {
            file: Arc::new(file),
            uring_system,
            next_free_block: AtomicU64::new(0),
        })
    }

    // File cache management

    pub async fn read_block(
        &self,
        cache_block: CacheBlock,
        dst: impl uring_common::buf::IoBufMut + Send + Sync,
    ) -> Result<(), std::io::Error> {
        assert!(dst.bytes_total() == BLCKSZ);
        let file = self.file.clone();

        let ((_file, _buf), res) = self
            .uring_system
            .read(file, cache_block as u64 * BLCKSZ as u64, dst)
            .await;

        let res = res.map_err(map_io_uring_error)?;
        if res != BLCKSZ {
            panic!("unexpected read result");
        }

        Ok(())
    }

    pub async fn write_block(
        &self,
        cache_block: CacheBlock,
        src: impl uring_common::buf::IoBuf + Send + Sync,
    ) -> Result<(), std::io::Error> {
        assert!(src.bytes_init() == BLCKSZ);
        let file = self.file.clone();

        let ((_file, _buf), res) = self
            .uring_system
            .write(file, cache_block as u64 * BLCKSZ as u64, src)
            .await;
        let res = res.map_err(map_io_uring_error)?;
        if res != BLCKSZ {
            panic!("unexpected read result");
        }

        Ok(())
    }

    pub fn alloc_block(&self) -> CacheBlock {
        self.next_free_block.fetch_add(1, Ordering::Relaxed)
    }
}

fn map_io_uring_error(err: tokio_epoll_uring::Error<std::io::Error>) -> std::io::Error {
    match err {
        tokio_epoll_uring::Error::Op(err) => err,
        tokio_epoll_uring::Error::System(err) => {
            std::io::Error::new(std::io::ErrorKind::Other, err)
        }
    }
}
