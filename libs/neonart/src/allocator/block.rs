//! Simple allocator of fixed-size blocks

use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

use spin;

const BLOCK_SIZE: usize = 16*1024;

const INVALID_BLOCK: u64 = u64::MAX;

pub(crate) struct BlockAllocator {
    blocks_ptr: *mut MaybeUninit<u8>,
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


impl BlockAllocator {
    pub(crate) fn new(ptr: *mut MaybeUninit<u8>, size: usize) -> Self {
        let mut p = ptr;
        // Use all the space for the blocks
        let padding = p.align_offset(BLOCK_SIZE);
        p = unsafe { p.byte_add(padding) };
        let blocks_ptr = p;

        let used = unsafe { p.byte_offset_from(ptr) as usize };
        assert!(used <= size);
        let blocks_size = size - used;

        let num_blocks = (blocks_size / BLOCK_SIZE) as u64;

        BlockAllocator {
            blocks_ptr,
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
        unsafe { self.blocks_ptr.byte_offset(blkno as isize * BLOCK_SIZE as isize) }.cast()
    }

    pub(crate) fn alloc_block(&self) -> *mut u8 {
        self.get_block_ptr(self.alloc_block_internal())
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
            match self.num_initialized.compare_exchange(next_uninitialized, next_uninitialized + 1, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    return next_uninitialized;
                },
                Err(old) => {
                    next_uninitialized = old;
                    continue;
                },
            }
        }

        // out of blocks
        return INVALID_BLOCK;
    }

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
}
