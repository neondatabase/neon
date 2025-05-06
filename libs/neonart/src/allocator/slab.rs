use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};

use spin;

use super::alloc_from_slice;
use super::block::BlockAllocator;

use crate::allocator::block::BLOCK_SIZE;

pub(crate) struct SlabDesc {
    pub(crate) layout: Layout,

    block_lists: spin::RwLock<BlockLists>,
}

unsafe impl Sync for SlabDesc {}
unsafe impl Send for SlabDesc {}

#[derive(Default, Debug)]
struct BlockLists {
    full_blocks: BlockList,
    nonfull_blocks: BlockList,
}

#[derive(Default, Debug)]
struct BlockList {
    head: *mut SlabBlockHeader,
    tail: *mut SlabBlockHeader,
}

impl BlockList {
    unsafe fn push_head(&mut self, elem: *mut SlabBlockHeader) {
        unsafe {
            (*elem).next = self.head;
            if self.is_empty() {
                self.tail = elem;
                (*elem).next = std::ptr::null_mut();
            } else {
                (*elem).next = self.head;
            }
            (*elem).prev = std::ptr::null_mut();
            self.head = elem;
        }
    }

    fn is_empty(&self) -> bool {
        self.head.is_null()
    }

    unsafe fn unlink(&mut self, elem: *mut SlabBlockHeader) {
        unsafe {
            if (*elem).next.is_null() {
                assert!(self.tail == elem);
                self.tail = (*elem).prev;
            } else {
                assert!((*(*elem).next).prev == elem);
                (*(*elem).next).prev = (*elem).prev;
            }
            if (*elem).prev.is_null() {
                assert!(self.head == elem);
                self.head = (*elem).next;
            } else {
                assert!((*(*elem).prev).next == elem);
                (*(*elem).prev).next = (*elem).next;
            }
        }
    }
}

impl SlabDesc {
    pub(crate) fn new(layout: &Layout) -> SlabDesc {
        SlabDesc {
            layout: *layout,
            block_lists: spin::RwLock::new(BlockLists::default()),
        }
    }
}

#[derive(Debug)]
struct SlabBlockHeader {
    free_chunks_head: spin::Mutex<*mut FreeChunk>,
    num_free_chunks: AtomicU32,
    num_chunks: u32, // this is really a constant for a given Layout

    // these are valid when this block is in the 'nonfull_blocks' list
    prev: *mut SlabBlockHeader,
    next: *mut SlabBlockHeader,
}

struct FreeChunk {
    next: *mut FreeChunk,
}

enum ReadOrWriteGuard<'a, T> {
    Read(spin::RwLockReadGuard<'a, T>),
    Write(spin::RwLockWriteGuard<'a, T>),
}

impl<'a, T> Deref for ReadOrWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &<Self as Deref>::Target {
        match self {
            ReadOrWriteGuard::Read(g) => g.deref(),
            ReadOrWriteGuard::Write(g) => g.deref(),
        }
    }
}

impl SlabDesc {
    pub fn alloc_chunk(&self, block_allocator: &BlockAllocator) -> *mut u8 {
        // Are there any free chunks?
        let mut acquire_write = false;
        loop {
            let mut block_lists_guard = if acquire_write {
                ReadOrWriteGuard::Write(self.block_lists.write())
            } else {
                ReadOrWriteGuard::Read(self.block_lists.read())
            };
            let block_ptr = block_lists_guard.nonfull_blocks.head;
            if block_ptr.is_null() {
                break;
            }
            unsafe {
                let mut free_chunks_head = (*block_ptr).free_chunks_head.lock();
                if !(*free_chunks_head).is_null() {
                    let result = *free_chunks_head;
                    (*free_chunks_head) = (*result).next;
                    (*block_ptr).num_free_chunks.fetch_sub(1, Ordering::Relaxed);
                    return result.cast();
                }
            }

            // The block at the head of the list was full. Grab write lock and retry
            match block_lists_guard {
                ReadOrWriteGuard::Read(_) => {
                    acquire_write = true;
                    continue;
                }
                ReadOrWriteGuard::Write(ref mut g) => {
                    // move the node to the list of full blocks
                    unsafe {
                        g.nonfull_blocks.unlink(block_ptr);
                        g.full_blocks.push_head(block_ptr);
                    };
                    break;
                }
            }
        }

        // no free chunks. Allocate a new block (and the chunk from that)
        let (new_block, new_chunk) = self.alloc_block_and_chunk(block_allocator);

        // Add the block to the list in the SlabDesc
        unsafe {
            let mut block_lists_guard = self.block_lists.write();
            block_lists_guard.nonfull_blocks.push_head(new_block);
        }

        new_chunk
    }

    pub fn dealloc_chunk(&self, chunk_ptr: *mut u8, _block_allocator: &BlockAllocator) {
        // Find the block it belongs to. You can find the block from the address. (And knowing the
        // layout, you could calculate the chunk number too.)
        let block_ptr: *mut SlabBlockHeader = {
            let block_addr = (chunk_ptr.addr() / BLOCK_SIZE) * BLOCK_SIZE;
            chunk_ptr.with_addr(block_addr).cast()
        };
        let chunk_ptr: *mut FreeChunk = chunk_ptr.cast();

        // Mark the chunk as free in 'freechunks' list
        let num_chunks;
        let num_free_chunks;
        unsafe {
            let mut free_chunks_head = (*block_ptr).free_chunks_head.lock();
            (*chunk_ptr).next = *free_chunks_head;
            *free_chunks_head = chunk_ptr;

            num_free_chunks = (*block_ptr).num_free_chunks.fetch_add(1, Ordering::Relaxed) + 1;
            num_chunks = (*block_ptr).num_chunks;
        }

        if num_free_chunks == 1 {
            // If the block was full previously, add it to the nonfull blocks list. Note that
            // we're not holding the lock anymore, so it can immediately become full again
            // TODO
        } else if num_free_chunks == num_chunks {
            // If the block became completely empty, move it to the free list
            // TODO
            // FIXME: we're still holding the spinlock. It's not exactly safe to return it to
            // the free blocks list, is it? Defer it as garbage to wait out concurrent updates?
            //block_allocator.release_block()
        }
    }

    fn alloc_block_and_chunk(
        &self,
        block_allocator: &BlockAllocator,
    ) -> (*mut SlabBlockHeader, *mut u8) {
        // fixme: handle OOM
        let block_slice: &mut [MaybeUninit<u8>] = block_allocator.alloc_block();
        let (block_header, remain) = alloc_from_slice::<SlabBlockHeader>(block_slice);

        let padding = remain.as_ptr().align_offset(self.layout.align());

        let num_chunks = (remain.len() - padding) / self.layout.size();

        let first_chunk_ptr: *mut FreeChunk = remain[padding..].as_mut_ptr().cast();

        unsafe {
            let mut chunk_ptr = first_chunk_ptr;
            for _ in 0..num_chunks - 1 {
                let next_chunk_ptr = chunk_ptr.byte_add(self.layout.size());
                (*chunk_ptr).next = next_chunk_ptr;
                chunk_ptr = next_chunk_ptr;
            }
            (*chunk_ptr).next = std::ptr::null_mut();

            let result_chunk = first_chunk_ptr;

            let block_header = block_header.write(SlabBlockHeader {
                free_chunks_head: spin::Mutex::new((*first_chunk_ptr).next),
                prev: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
                num_chunks: num_chunks as u32,
                num_free_chunks: AtomicU32::new(num_chunks as u32 - 1),
            });

            (block_header, result_chunk.cast())
        }
    }
}
