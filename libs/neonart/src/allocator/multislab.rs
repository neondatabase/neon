use std::alloc::Layout;
use std::mem::MaybeUninit;

use crate::allocator::block::BlockAllocator;
use crate::allocator::slab::SlabDesc;
use crate::allocator::r#static::StaticAllocator;

pub struct MultiSlabAllocator<'t, const N: usize> {
    pub(crate) block_allocator: BlockAllocator<'t>,

    pub(crate) slab_descs: [SlabDesc; N],
}

unsafe impl<'t, const N: usize> Sync for MultiSlabAllocator<'t, N> {}
unsafe impl<'t, const N: usize> Send for MultiSlabAllocator<'t, N> {}

impl<'t, const N: usize> MultiSlabAllocator<'t, N> {
    pub(crate) fn new(
        area: &'t mut [MaybeUninit<u8>],
        layouts: &[Layout; N],
    ) -> &'t mut MultiSlabAllocator<'t, N> {
        // Set up the MultiSlabAllocator struct in the area first
        let mut allocator = StaticAllocator::new(area);

        let this = allocator.alloc_uninit();

        let block_allocator = BlockAllocator::new(allocator.remaining());

        let this = this.write(MultiSlabAllocator {
            block_allocator,

            slab_descs: std::array::from_fn(|i| SlabDesc::new(&layouts[i])),
        });

        this
    }

    pub(crate) fn alloc_fit(&self, layout: Layout) -> *mut u8 {
        for i in 0..self.slab_descs.len() {
            if self.slab_descs[i].layout.align() >= layout.align()
                && self.slab_descs[i].layout.size() >= layout.size()
            {
                return self.alloc_slab(i);
            }
        }
        panic!("no suitable slab found for allocation");
    }

    pub(crate) fn alloc_slab(&self, slab_idx: usize) -> *mut u8 {
        self.slab_descs[slab_idx].alloc_chunk(&self.block_allocator)
    }

    pub(crate) fn dealloc_slab(&self, slab_idx: usize, ptr: *mut u8) {
        self.slab_descs[slab_idx].dealloc_chunk(ptr, &self.block_allocator)
    }
}
