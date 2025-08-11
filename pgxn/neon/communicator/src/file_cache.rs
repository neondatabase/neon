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
use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use measured::metric;
use measured::metric::MetricEncoding;
use measured::metric::gauge::GaugeState;
use measured::{Gauge, MetricGroup};

use crate::BLCKSZ;

use tokio::task::spawn_blocking;

pub type CacheBlock = u64;

pub const INVALID_CACHE_BLOCK: CacheBlock = u64::MAX;

pub struct FileCache {
    file: Arc<File>,
    free_list: Mutex<FreeList>,
	pub size: AtomicU64,
	max_read: AtomicU64,
	
	// The `fiemap-rs` library doesn't expose any way to issue a FIEMAP ioctl
	// on an existing file descroptor, so we have to save the path.
	path: PathBuf,

	pub max_written: AtomicU64,
    metrics: FileCacheMetricGroup,
}

#[derive(MetricGroup)]
#[metric(new())]
struct FileCacheMetricGroup {
    /// Local File Cache size in 8KiB blocks
    max_blocks: Gauge,

    /// Number of free 8KiB blocks in Local File Cache
    num_free_blocks: Gauge,
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

        tracing::info!("initialized file cache with {} blocks", initial_size);

        Ok(FileCache {
			max_read: 0.into(),
            file: Arc::new(file),
			size: initial_size.into(),
            free_list: Mutex::new(FreeList {
                next_free_block: 0,
                max_blocks: initial_size,
                free_blocks: Vec::new(),
            }),
			max_written: 0.into(),
			path: file_cache_path.to_path_buf(),
            metrics: FileCacheMetricGroup::new(),
        })
    }

	/// Debug utility.
	pub fn num_allocated_blocks(&self) -> (u64, u32) {
		(
			std::fs::metadata(self.path.clone()).unwrap().st_size() / (1024 * 1024),
			String::from_utf8_lossy(
				&std::process::Command::new("ls")
					.args(["-sk", self.path.to_str().unwrap()])
					.output()
					.expect("failed to run ls -sk")
					.stdout
			).split(' ').next().unwrap().parse().unwrap()
		)
	}
	
    // File cache management

    pub async fn read_block(
        &self,
        cache_block: CacheBlock,
        mut dst: impl uring_common::buf::IoBufMut + Send + Sync,
    ) -> Result<(), std::io::Error> {
        assert!(dst.bytes_total() == BLCKSZ);
        let file = self.file.clone();

		let prev = self.max_read.fetch_max(cache_block, Ordering::Relaxed);
		if prev.max(cache_block) > self.free_list.lock().unwrap().max_blocks {
			tracing::info!("max read greater than max block!! {cache_block}");
		}
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

		self.max_written.fetch_max(cache_block, Ordering::Relaxed);
		
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

	pub fn reclaim_blocks(&self, num_blocks: u64) -> u64 {
		let mut free_list = self.free_list.lock().unwrap();
		tracing::warn!("next block: {},  max block: {}", free_list.next_free_block, free_list.max_blocks);
		let removed = (free_list.free_blocks.len() as u64).min(num_blocks);
		for _ in 0..removed {
			self.delete_block(free_list.free_blocks.pop().unwrap());
		}
		tracing::warn!("punched {removed} individual holes");
		tracing::warn!("avail block space is: {}", free_list.max_blocks - free_list.next_free_block);
		let block_space = (num_blocks - removed).min(free_list.max_blocks - free_list.next_free_block);
		
		free_list.max_blocks -= block_space;
		self.size.fetch_sub(block_space, Ordering::Relaxed);
		tracing::warn!("punched a large hole of size {block_space}");
		
		num_blocks - block_space - removed
	}
	
	/// "Delete" a block via fallocate's hole punching feature.
	// TODO(quantumish): possibly implement some batching? lots of syscalls...
	// unfortunately should be at odds with our access pattern as entries in the hashmap
	// should have no correlation with the location of blocks in the actual LFC file.
	pub fn delete_block(&self, cache_block: CacheBlock) {		
		self.delete_blocks(cache_block, 1);
	}

	pub fn delete_blocks(&self, start: CacheBlock, amt: u64) {
		use nix::fcntl as nix;
		if amt == 0 { return }
		self.size.fetch_sub(amt, Ordering::Relaxed);
		if let Err(e) = nix::fallocate(
			self.file.clone(),
			nix::FallocateFlags::FALLOC_FL_PUNCH_HOLE
				.union(nix::FallocateFlags::FALLOC_FL_KEEP_SIZE),
			(start as usize * BLCKSZ) as libc::off_t,
			(amt as usize * BLCKSZ) as libc::off_t
		) {
			tracing::error!("failed to punch {amt} hole(s) in LFC at block {start}: {e}");
			return;
		}
	}

	/// Attempt to reclaim `num_blocks` of previously hole-punched blocks.
	#[cfg(target_os = "linux")]
	pub fn undelete_blocks(&self, num_blocks: u64) -> u64 {
		use itertools::Itertools;
		let mut pushed = 0;
		let mut free_list = self.free_list.lock().unwrap();
		let res = fiemap::fiemap(self.path.as_path()).unwrap();
		for (prev, cur) in res.map(|x| x.unwrap()).tuple_windows() {
			if (prev.fe_logical + prev.fe_length) < cur.fe_logical {
				let mut end = prev.fe_logical + prev.fe_length;
				while end < cur.fe_logical {
					self.size.fetch_add(1, Ordering::Relaxed);
					free_list.free_blocks.push(end / BLCKSZ as u64);
					pushed += 1;
					if pushed == num_blocks {
						return 0;
					}
					end += BLCKSZ as u64;
				}
			}
		}
		num_blocks - pushed
	}
	
	/// Attempt to reclaim `num_blocks` of previously hole-punched blocks.
	// FIXME(quantumish): local tests showed this code has some buggy behavior.
	#[cfg(target_os = "macos")]
	pub fn undelete_blocks(&self, num_blocks: u64) -> u64 {
		use nix::unistd as nix;
		let mut free_list = self.free_list.lock().unwrap();
		let num_bytes = (free_list.next_free_block * BLOCKSZ) as i64;
		let mut cur_pos = 0;
		let mut pushed = 0;
		while cur_pos < num_bytes {
			let res = nix::lseek(
				file.clone(),
				cur_pos,
				nix::Whence::SeekHole
			).unwrap();
			if res >= num_bytes {
				break;
			}
			self.size.fetch_add(1, Ordering::Relaxed);
			free_list.free_blocks.push(res);
			pushed += 1;
			if pushed == num_blocks {
				return 0;
			}
			cur_pos = res + BLOCKSZ as i64;
		}
		num_blocks - pushed
	}

	/// Physically grows the file and expands the freelist.
	pub fn grow(&self, num_blocks: u64) {
		let mut free_list = self.free_list.lock().unwrap();
		// self.allocate_blocks(free_list.max_blocks, num_blocks);
		self.size.fetch_add(num_blocks, Ordering::Relaxed);
		free_list.max_blocks += num_blocks;
		tracing::warn!("(after) max block: {}", free_list.max_blocks);
	}

	/// Returns number of blocks in the remaining space.
	pub fn free_space(&self) -> u64 {
		let free_list = self.free_list.lock().unwrap();
		let slab = free_list.max_blocks - free_list.next_free_block.min(free_list.max_blocks);
		let fragments = free_list.free_blocks.len() as u64;
		slab + fragments
	}
}

impl<T: metric::group::Encoding> MetricGroup<T> for FileCache
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), <T as metric::group::Encoding>::Err> {
        // Update the gauges with fresh values first
        {
            let free_list = self.free_list.lock().unwrap();
            self.metrics.max_blocks.set(free_list.max_blocks as i64);

            let total_free_blocks: i64 = free_list.free_blocks.len() as i64
                + (free_list.max_blocks as i64 - free_list.next_free_block as i64);
            self.metrics.num_free_blocks.set(total_free_blocks);
        }

        self.metrics.collect_group_into(enc)
    }
}
