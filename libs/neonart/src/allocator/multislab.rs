use std::alloc::Layout;
use std::mem::MaybeUninit;

use crate::allocator::block::BlockAllocator;
use crate::allocator::slab::SlabDesc;

pub struct MultiSlabAllocator<'t, const N: usize> {
    pub(crate) block_allocator: BlockAllocator<'t>,

    pub(crate) slab_descs: [SlabDesc; N],
}

impl<'t, const N: usize> MultiSlabAllocator<'t, N> {
    pub(crate) fn new(
        area: &'t mut [MaybeUninit<u8>],
        layouts: &[Layout; N],
    ) -> MultiSlabAllocator<'t, N> {
        let block_allocator = BlockAllocator::new(area);
        MultiSlabAllocator {
            block_allocator,

            slab_descs: std::array::from_fn(|i| SlabDesc::new(&layouts[i])),
        }
    }

    pub(crate) fn alloc_slab(&self, slab_idx: usize) -> *mut u8 {
        self.slab_descs[slab_idx].alloc_chunk(&self.block_allocator)
    }

    pub(crate) fn dealloc_slab(&self, slab_idx: usize, ptr: *mut u8) {
        self.slab_descs[slab_idx].dealloc_chunk(ptr, &self.block_allocator)
    }
}
