//! A slab allocator that carves out fixed-size chunks from larger blocks.
//!
//!

use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use spin;

use super::alloc_from_slice;
use super::block::BlockAllocator;

use crate::allocator::block::BLOCK_SIZE;

pub(crate) struct SlabDesc {
    pub(crate) layout: Layout,

    block_lists: spin::RwLock<BlockLists>,

    pub(crate) num_blocks: AtomicU64,
    pub(crate) num_allocated: AtomicU64,
}

// FIXME: Not sure if SlabDesc is really Sync or Send. It probably is when it's empty, but
// 'block_lists' contains pointers when it's not empty. In the current use as part of the
// the art tree, SlabDescs are only moved during initialization.
unsafe impl Sync for SlabDesc {}
unsafe impl Send for SlabDesc {}

#[derive(Default, Debug)]
struct BlockLists {
    full_blocks: BlockList,
    nonfull_blocks: BlockList,
}

impl BlockLists {
    // Unlink a node. It must be in either one of the two lists.
    unsafe fn unlink(&mut self, elem: *mut SlabBlockHeader) {
        let list = unsafe {
            if (*elem).next.is_null() {
                if self.full_blocks.tail == elem {
                    Some(&mut self.full_blocks)
                } else {
                    Some(&mut self.nonfull_blocks)
                }
            } else if (*elem).prev.is_null() {
                if self.full_blocks.head == elem {
                    Some(&mut self.full_blocks)
                } else {
                    Some(&mut self.nonfull_blocks)
                }
            } else {
                None
            }
        };
        unsafe { unlink_slab_block(list, elem) };
    }
}

unsafe fn unlink_slab_block(mut list: Option<&mut BlockList>, elem: *mut SlabBlockHeader) {
    unsafe {
        if (*elem).next.is_null() {
            assert_eq!(list.as_ref().unwrap().tail, elem);
            list.as_mut().unwrap().tail = (*elem).prev;
        } else {
            assert_eq!((*(*elem).next).prev, elem);
            (*(*elem).next).prev = (*elem).prev;
        }
        if (*elem).prev.is_null() {
            assert_eq!(list.as_ref().unwrap().head, elem);
            list.as_mut().unwrap().head = (*elem).next;
        } else {
            assert_eq!((*(*elem).prev).next, elem);
            (*(*elem).prev).next = (*elem).next;
        }
    }
}

#[derive(Debug)]
struct BlockList {
    head: *mut SlabBlockHeader,
    tail: *mut SlabBlockHeader,
}

impl Default for BlockList {
    fn default() -> Self {
        BlockList {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
        }
    }
}

impl BlockList {
    unsafe fn push_head(&mut self, elem: *mut SlabBlockHeader) {
        unsafe {
            if self.is_empty() {
                self.tail = elem;
                (*elem).next = std::ptr::null_mut();
            } else {
                (*elem).next = self.head;
                (*self.head).prev = elem;
            }
            (*elem).prev = std::ptr::null_mut();
            self.head = elem;
        }
    }

    fn is_empty(&self) -> bool {
        self.head.is_null()
    }

    unsafe fn unlink(&mut self, elem: *mut SlabBlockHeader) {
        unsafe { unlink_slab_block(Some(self), elem) }
    }

    #[cfg(test)]
    fn dump(&self) {
        let mut next = self.head;

        while !next.is_null() {
            let n = unsafe { next.as_ref() }.unwrap();
            eprintln!(
                "  blk {:?} (free {}/{})",
                next,
                n.num_free_chunks.load(Ordering::Relaxed),
                n.num_chunks
            );
            next = n.next;
        }
    }
}

impl SlabDesc {
    pub(crate) fn new(layout: &Layout) -> SlabDesc {
        SlabDesc {
            layout: *layout,
            block_lists: spin::RwLock::new(BlockLists::default()),
            num_allocated: AtomicU64::new(0),
            num_blocks: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
struct SlabBlockHeader {
    free_chunks_head: spin::Mutex<*mut FreeChunk>,
    num_free_chunks: AtomicU32,
    num_chunks: u32, // this is really a constant for a given Layout

    // these fields are protected by the lock on the BlockLists
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
        'outer: loop {
            let mut block_lists_guard = if acquire_write {
                ReadOrWriteGuard::Write(self.block_lists.write())
            } else {
                ReadOrWriteGuard::Read(self.block_lists.read())
            };
            'inner: loop {
                let block_ptr = block_lists_guard.nonfull_blocks.head;
                if block_ptr.is_null() {
                    break 'outer;
                }
                unsafe {
                    let mut free_chunks_head = (*block_ptr).free_chunks_head.lock();
                    if !(*free_chunks_head).is_null() {
                        let result = *free_chunks_head;
                        (*free_chunks_head) = (*result).next;
                        let _old = (*block_ptr).num_free_chunks.fetch_sub(1, Ordering::Relaxed);

                        self.num_allocated.fetch_add(1, Ordering::Relaxed);
                        return result.cast();
                    }
                }

                // The block at the head of the list was full. Grab write lock and retry
                match block_lists_guard {
                    ReadOrWriteGuard::Read(_) => {
                        acquire_write = true;
                        continue 'outer;
                    }
                    ReadOrWriteGuard::Write(ref mut g) => {
                        // move the node to the list of full blocks
                        unsafe {
                            g.nonfull_blocks.unlink(block_ptr);
                            g.full_blocks.push_head(block_ptr);
                        };
                        continue 'inner;
                    }
                }
            }
        }

        // no free chunks. Allocate a new block (and the chunk from that)
        let (new_block, new_chunk) = self.alloc_block_and_chunk(block_allocator);
        self.num_blocks.fetch_add(1, Ordering::Relaxed);

        // Add the block to the list in the SlabDesc
        unsafe {
            let mut block_lists_guard = self.block_lists.write();
            block_lists_guard.nonfull_blocks.push_head(new_block);
        }
        self.num_allocated.fetch_add(1, Ordering::Relaxed);
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
            // we're not holding the lock anymore, so it can immediately become full again.
            // That's harmless, it will be moved back to the full list again when a call
            // to alloc_chunk() sees it.
            let mut block_lists = self.block_lists.write();
            unsafe {
                block_lists.unlink(block_ptr);
                block_lists.nonfull_blocks.push_head(block_ptr);
            };
        } else if num_free_chunks == num_chunks {
            // If the block became completely empty, move it to the free list
            // TODO
            // FIXME: we're still holding the spinlock. It's not exactly safe to return it to
            // the free blocks list, is it? Defer it as garbage to wait out concurrent updates?
            //block_allocator.release_block()
        }

        // update stats
        self.num_allocated.fetch_sub(1, Ordering::Relaxed);
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

    #[cfg(test)]
    fn dump(&self) {
        eprintln!(
            "slab dump ({} blocks, {} allocated chunks)",
            self.num_blocks.load(Ordering::Relaxed),
            self.num_allocated.load(Ordering::Relaxed)
        );
        let lists = self.block_lists.read();

        eprintln!("nonfull blocks:");
        lists.nonfull_blocks.dump();
        eprintln!("full blocks:");
        lists.full_blocks.dump();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::Rng;
    use rand_distr::Zipf;

    struct TestObject {
        val: usize,
        _dummy: [u8; BLOCK_SIZE / 4],
    }

    struct TestObjectSlab<'a>(SlabDesc, BlockAllocator<'a>);
    impl<'a> TestObjectSlab<'a> {
        fn new(block_allocator: BlockAllocator) -> TestObjectSlab {
            TestObjectSlab(SlabDesc::new(&Layout::new::<TestObject>()), block_allocator)
        }

        fn alloc(&self, val: usize) -> *mut TestObject {
            let obj: *mut TestObject = self.0.alloc_chunk(&self.1).cast();
            unsafe { (*obj).val = val };
            obj
        }

        fn dealloc(&self, obj: *mut TestObject) {
            self.0.dealloc_chunk(obj.cast(), &self.1)
        }
    }

    #[test]
    fn test_slab_alloc() {
        const MEM_SIZE: usize = 100000000;
        let mut area = Box::new_uninit_slice(MEM_SIZE);
        let block_allocator = BlockAllocator::new(&mut area);

        let slab = TestObjectSlab::new(block_allocator);

        let mut all: Vec<*mut TestObject> = Vec::new();
        for i in 0..11 {
            all.push(slab.alloc(i));
        }
        #[allow(clippy::needless_range_loop)]
        for i in 0..11 {
            assert!(unsafe { (*all[i]).val == i });
        }

        let distribution = Zipf::new(10.0, 1.1).unwrap();
        let mut rng = rand::rng();
        for _ in 0..100000 {
            slab.0.dump();
            let idx = rng.sample(distribution) as usize;
            let ptr: *mut TestObject = all[idx];
            if !ptr.is_null() {
                assert_eq!(unsafe { (*ptr).val }, idx);
                slab.dealloc(ptr);
                all[idx] = std::ptr::null_mut();
            } else {
                all[idx] = slab.alloc(idx);
            }
        }
    }

    fn new_test_blk(i: u32) -> *mut SlabBlockHeader {
        Box::into_raw(Box::new(SlabBlockHeader {
            free_chunks_head: spin::Mutex::new(std::ptr::null_mut()),
            num_free_chunks: AtomicU32::new(0),
            num_chunks: i,
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        }))
    }

    #[test]
    fn test_block_linked_list() {
        // note: these are leaked, but that's OK for tests
        let a = new_test_blk(0);
        let b = new_test_blk(1);

        let mut list = BlockList::default();
        assert!(list.is_empty());

        unsafe {
            list.push_head(a);
            assert!(!list.is_empty());
            list.unlink(a);
        }
        assert!(list.is_empty());

        unsafe {
            list.push_head(b);
            list.push_head(a);
            assert_eq!(list.head, a);
            assert_eq!((*a).next, b);
            assert_eq!((*b).prev, a);
            assert_eq!(list.tail, b);

            list.unlink(a);
            list.unlink(b);
            assert!(list.is_empty());
        }
    }
}
