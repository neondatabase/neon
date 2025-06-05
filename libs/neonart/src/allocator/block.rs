//! Simple allocator of fixed-size blocks

use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

use spin;

pub const BLOCK_SIZE: usize = 16 * 1024;

const INVALID_BLOCK: u64 = u64::MAX;

pub(crate) struct BlockAllocator<'t> {
    blocks_ptr: &'t [MaybeUninit<u8>],
    num_blocks: u64,
    num_initialized: AtomicU64,

    freelist_head: spin::Mutex<u64>,
}

struct FreeListBlock {
    inner: spin::Mutex<FreeListBlockInner>,
}

struct FreeListBlockInner {
    next: u64,

    num_free_blocks: u64,
    free_blocks: [u64; 100], // FIXME: fill the rest of the block
}

impl<'t> BlockAllocator<'t> {
    pub(crate) fn new(area: &'t mut [MaybeUninit<u8>]) -> Self {
        // Use all the space for the blocks
        let padding = area.as_ptr().align_offset(BLOCK_SIZE);
        let remain = &mut area[padding..];

        let num_blocks = (remain.len() / BLOCK_SIZE) as u64;

        BlockAllocator {
            blocks_ptr: remain,
            num_blocks,
            num_initialized: AtomicU64::new(0),
            freelist_head: spin::Mutex::new(INVALID_BLOCK),
        }
    }

    /// safety: you must hold a lock on the pointer to this block, otherwise it might get
    /// reused for another kind of block
    fn read_freelist_block(&self, blkno: u64) -> &FreeListBlock {
        let ptr: *const FreeListBlock = self.get_block_ptr(blkno).cast();
        unsafe { ptr.as_ref().unwrap() }
    }

    fn get_block_ptr(&self, blkno: u64) -> *mut u8 {
        assert!(blkno < self.num_blocks);
        unsafe {
            self.blocks_ptr
                .as_ptr()
                .byte_offset(blkno as isize * BLOCK_SIZE as isize)
        }
        .cast_mut()
        .cast()
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn alloc_block(&self) -> &mut [MaybeUninit<u8>] {
        // FIXME: handle OOM
        let blkno = self.alloc_block_internal();
        if blkno == INVALID_BLOCK {
            panic!("out of memory");
        }

        let ptr: *mut MaybeUninit<u8> = self.get_block_ptr(blkno).cast();
        unsafe { std::slice::from_raw_parts_mut(ptr, BLOCK_SIZE) }
    }

    fn alloc_block_internal(&self) -> u64 {
        //  check the free list.
        {
            let mut freelist_head = self.freelist_head.lock();
            if *freelist_head != INVALID_BLOCK {
                let freelist_block = self.read_freelist_block(*freelist_head);

                // acquire lock on the freelist block before releasing the lock on the parent (i.e. lock coupling)
                let mut g = freelist_block.inner.lock();

                if g.num_free_blocks > 0 {
                    g.num_free_blocks -= 1;
                    let result = g.free_blocks[g.num_free_blocks as usize];
                    return result;
                } else {
                    // consume the freelist block itself
                    let result = *freelist_head;
                    *freelist_head = g.next;
                    // This freelist block is now unlinked and can be repurposed
                    drop(g);
                    return result;
                }
            }
        }

        // If there are some blocks left that we've never used, pick next such block
        let mut next_uninitialized = self.num_initialized.load(Ordering::Relaxed);
        while next_uninitialized < self.num_blocks {
            match self.num_initialized.compare_exchange(
                next_uninitialized,
                next_uninitialized + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return next_uninitialized;
                }
                Err(old) => {
                    next_uninitialized = old;
                    continue;
                }
            }
        }

        // out of blocks
        return INVALID_BLOCK;
    }

    // TODO: this is currently unused. The slab allocator never releases blocks
    #[allow(dead_code)]
    pub(crate) fn release_block(&self, block_ptr: *mut u8) {
        let blockno = unsafe { block_ptr.byte_offset_from(self.blocks_ptr) / BLOCK_SIZE as isize };
        self.release_block_internal(blockno as u64);
    }

    fn release_block_internal(&self, blockno: u64) {
        let mut freelist_head = self.freelist_head.lock();
        if *freelist_head != INVALID_BLOCK {
            let freelist_block = self.read_freelist_block(*freelist_head);

            // acquire lock on the freelist block before releasing the lock on the parent (i.e. lock coupling)
            let mut g = freelist_block.inner.lock();

            let num_free_blocks = g.num_free_blocks;
            if num_free_blocks < g.free_blocks.len() as u64 {
                g.free_blocks[num_free_blocks as usize] = blockno;
                g.num_free_blocks += 1;
                return;
            }
        }

        // Convert the block into a new freelist block
        let block_ptr: *mut FreeListBlock = self.get_block_ptr(blockno).cast();
        let init = FreeListBlock {
            inner: spin::Mutex::new(FreeListBlockInner {
                next: *freelist_head,
                num_free_blocks: 0,
                free_blocks: [INVALID_BLOCK; 100],
            }),
        };
        unsafe { (*block_ptr) = init };
        *freelist_head = blockno;
    }

    // for debugging
    pub(crate) fn get_statistics(&self) -> BlockAllocatorStats {
        let mut num_free_blocks = 0;

        let mut _prev_lock = None;
        let head_lock = self.freelist_head.lock();
        let mut next_blk = *head_lock;
        let mut _head_lock = Some(head_lock);
        while next_blk != INVALID_BLOCK {
            let freelist_block = self.read_freelist_block(next_blk);
            let lock = freelist_block.inner.lock();
            num_free_blocks += lock.num_free_blocks;
            next_blk = lock.next;
            _prev_lock = Some(lock); // hold the lock until we've read the next block
            _head_lock = None;
        }

        BlockAllocatorStats {
            num_blocks: self.num_blocks,
            num_initialized: self.num_initialized.load(Ordering::Relaxed),
            num_free_blocks,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlockAllocatorStats {
    pub num_blocks: u64,
    pub num_initialized: u64,
    pub num_free_blocks: u64,
}
