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
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use crate::BLCKSZ;

use tokio::task::spawn_blocking;

pub type CacheBlock = u64;

pub const INVALID_CACHE_BLOCK: CacheBlock = u64::MAX;

#[derive(Debug)]
pub struct FileCache {
    file: Arc<File>,

    free_list: Mutex<FreeList>,
	// NOTE(quantumish): when we punch holes in the LFC to shrink the file size,
	// we *shouldn't* add them to the free list (since that's used to write new entries)
	// but we still should remember them so that we can add them to the free list when
	// a growth later occurs. this still has the same scalability flaws as the freelist 
	hole_list: Mutex<Vec<CacheBlock>>,
	
    // metrics
    max_blocks_gauge: metrics::IntGauge,
    num_free_blocks_gauge: metrics::IntGauge,
}

// TODO: We keep track of all free blocks in this vec. That doesn't really scale.
// Idea: when free_blocks fills up with more than 1024 entries, write them all to
// one block on disk.
#[derive(Debug)]
struct FreeList {
    next_free_block: CacheBlock,
    max_blocks: u64,

    free_blocks: Vec<CacheBlock>,
}

impl FileCache {
    pub fn new(file_cache_path: &Path, mut initial_size: u64) -> Result<FileCache, std::io::Error> {
        if initial_size < 100 {
            tracing::warn!(
                "min size for file cache is 100 blocks, {} requested",
                initial_size
            );
            initial_size = 100;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(file_cache_path)?;

        let max_blocks_gauge = metrics::IntGauge::new(
            "file_cache_max_blocks",
            "Local File Cache size in 8KiB blocks",
        )
        .unwrap();
        let num_free_blocks_gauge = metrics::IntGauge::new(
            "file_cache_num_free_blocks",
            "Number of free 8KiB blocks in Local File Cache",
        )
        .unwrap();

        tracing::info!("initialized file cache with {} blocks", initial_size);

        Ok(FileCache {
            file: Arc::new(file),
            free_list: Mutex::new(FreeList {
                next_free_block: 0,
                max_blocks: initial_size,
                free_blocks: Vec::new(),
            }),
			hole_list: Mutex::new(Vec::new()),
            max_blocks_gauge,
            num_free_blocks_gauge,
        })
    }

    // File cache management

    pub async fn read_block(
        &self,
        cache_block: CacheBlock,
        mut dst: impl uring_common::buf::IoBufMut + Send + Sync,
    ) -> Result<(), std::io::Error> {
        assert!(dst.bytes_total() == BLCKSZ);
        let file = self.file.clone();

        let dst_ref = unsafe { std::slice::from_raw_parts_mut(dst.stable_mut_ptr(), BLCKSZ) };

        spawn_blocking(move || file.read_exact_at(dst_ref, cache_block * BLCKSZ as u64)).await??;
        Ok(())
    }

    pub async fn write_block(
        &self,
        cache_block: CacheBlock,
        src: impl uring_common::buf::IoBuf + Send + Sync,
    ) -> Result<(), std::io::Error> {
        assert!(src.bytes_init() == BLCKSZ);
        let file = self.file.clone();

        let src_ref = unsafe { std::slice::from_raw_parts(src.stable_ptr(), BLCKSZ) };

        spawn_blocking(move || file.write_all_at(src_ref, cache_block * BLCKSZ as u64)).await??;

        Ok(())
    }

    pub fn alloc_block(&self) -> Option<CacheBlock> {
        let mut free_list = self.free_list.lock().unwrap();
        if let Some(x) = free_list.free_blocks.pop() {
            return Some(x);
        }
        if free_list.next_free_block < free_list.max_blocks {
            let result = free_list.next_free_block;
            free_list.next_free_block += 1;
            return Some(result);
        }
        None
    }
	
    pub fn dealloc_block(&self, cache_block: CacheBlock) {
        let mut free_list = self.free_list.lock().unwrap();
        free_list.free_blocks.push(cache_block);
    }

	/// "Delete" a block via fallocate's hole punching feature.
	// TODO(quantumish): possibly implement some batching? lots of syscalls...
	// unfortunately should be at odds with our access pattern as entries in the hashmap
	// should have no correlation with the location of blocks in the actual LFC file.
	pub fn delete_block(&self, cache_block: CacheBlock) {
		use nix::fcntl as nix;
		if let Err(e) = nix::fallocate(
			self.file.clone(),
			nix::FallocateFlags::FALLOC_FL_PUNCH_HOLE
				.union(nix::FallocateFlags::FALLOC_FL_KEEP_SIZE),
			(cache_block as usize * BLCKSZ) as libc::off_t,
			BLCKSZ as libc::off_t
		) {
			tracing::error!("failed to punch hole in LFC at block {cache_block}: {e}");
			return;
		}

		let mut hole_list = self.hole_list.lock().unwrap();
        hole_list.push(cache_block);
	}

	/// Attempt to reclaim `num_blocks` of previously hole-punched blocks.
	// TODO(quantumish): could probably just be merged w/ grow() - is there ever a reason
	// to call this separately?
	pub fn undelete_blocks(&self, num_blocks: u64) -> u64 {
		// Safety: nothing else should ever need to take both of these locks at once.
		// TODO(quantumish): may just be worth putting both under the same lock.
		let mut hole_list = self.hole_list.lock().unwrap();
		let mut free_list = self.free_list.lock().unwrap();
		let amt = hole_list.len().min(num_blocks as usize);
		for _ in 0..amt {
			free_list.free_blocks.push(hole_list.pop().unwrap());
		}
		amt as u64
	}

	/// Physically grows the file and expands the freelist.
	pub fn grow(&self, num_blocks: u64) {
		self.free_list.lock().unwrap().max_blocks += num_blocks;
	}

	/// Returns number of blocks in the remaining space.
	pub fn free_space(&self) -> u64 {
		let free_list = self.free_list.lock().unwrap();
		let slab = free_list.max_blocks - free_list.next_free_block.min(free_list.max_blocks);
		let fragments = free_list.free_blocks.len() as u64;
		slab + fragments
	}
}

impl metrics::core::Collector for FileCache {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();
        descs.append(&mut self.max_blocks_gauge.desc());
        descs.append(&mut self.num_free_blocks_gauge.desc());
        descs
    }
    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        // Update the gauges with fresh values first
        {
            let free_list = self.free_list.lock().unwrap();
            self.max_blocks_gauge.set(free_list.max_blocks as i64);

            let total_free_blocks: i64 = free_list.free_blocks.len() as i64
                + (free_list.max_blocks as i64 - free_list.next_free_block as i64);
            self.num_free_blocks_gauge.set(total_free_blocks);
        }

        let mut values = Vec::new();
        values.append(&mut self.max_blocks_gauge.collect());
        values.append(&mut self.num_free_blocks_gauge.collect());
        values
    }
}
