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

use tokio_epoll_uring;

use std::sync::Mutex;

use crate::BLCKSZ;

pub type CacheBlock = u64;

pub struct FileCache {
    uring_system: tokio_epoll_uring::SystemHandle,

    file: Arc<File>,

    free_list: Mutex<FreeList>
}

// TODO: We keep track of all free blocks in this vec. That doesn't really scale.
struct FreeList {
    next_free_block: CacheBlock,
    max_blocks: u64,

    free_blocks: Vec<CacheBlock>,
}

impl FileCache {
    pub fn new(
        file_cache_path: &Path,
        initial_size: u64,
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
            free_list: Mutex::new(FreeList {
                next_free_block: 0,
                max_blocks: initial_size,
                free_blocks: Vec::new(),
            }),
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

    pub fn alloc_block(&self) -> Option<CacheBlock> {
        let mut free_list = self.free_list.lock().unwrap();
        if let Some(x) = free_list.free_blocks.pop() {
            return Some(x);
        }
        if free_list.next_free_block < free_list.max_blocks {
            let result = free_list.next_free_block;
            free_list.next_free_block -= 1;
            return Some(result);
        }
        None
    }

    pub fn dealloc_block(&self, cache_block: CacheBlock) {
        let mut free_list = self.free_list.lock().unwrap();
        free_list.free_blocks.push(cache_block);
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
