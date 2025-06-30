pub mod block;
mod multislab;
mod slab;
pub mod r#static;

use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

use crate::allocator::multislab::MultiSlabAllocator;
use crate::allocator::r#static::alloc_from_slice;

use spin;

use crate::Tree;
pub use crate::algorithm::node_ptr::{
    NodeInternal4, NodeInternal16, NodeInternal48, NodeInternal256, NodeLeaf,
};

#[derive(Debug)]
pub struct OutOfMemoryError();

pub trait ArtAllocator<V: crate::Value> {
    fn alloc_tree(&self) -> *mut Tree<V>;

    fn alloc_node_internal4(&self) -> *mut NodeInternal4<V>;
    fn alloc_node_internal16(&self) -> *mut NodeInternal16<V>;
    fn alloc_node_internal48(&self) -> *mut NodeInternal48<V>;
    fn alloc_node_internal256(&self) -> *mut NodeInternal256<V>;
    fn alloc_node_leaf(&self) -> *mut NodeLeaf<V>;

    fn dealloc_node_internal4(&self, ptr: *mut NodeInternal4<V>);
    fn dealloc_node_internal16(&self, ptr: *mut NodeInternal16<V>);
    fn dealloc_node_internal48(&self, ptr: *mut NodeInternal48<V>);
    fn dealloc_node_internal256(&self, ptr: *mut NodeInternal256<V>);
    fn dealloc_node_leaf(&self, ptr: *mut NodeLeaf<V>);
}

pub struct ArtMultiSlabAllocator<'t, V>
where
    V: crate::Value,
{
    tree_area: spin::Mutex<Option<&'t mut MaybeUninit<Tree<V>>>>,

    pub(crate) inner: MultiSlabAllocator<'t, 5>,

    phantom_val: PhantomData<V>,
}

impl<'t, V: crate::Value> ArtMultiSlabAllocator<'t, V> {
    const LAYOUTS: [Layout; 5] = [
        Layout::new::<NodeInternal4<V>>(),
        Layout::new::<NodeInternal16<V>>(),
        Layout::new::<NodeInternal48<V>>(),
        Layout::new::<NodeInternal256<V>>(),
        Layout::new::<NodeLeaf<V>>(),
    ];

    pub fn new(area: &'t mut [MaybeUninit<u8>]) -> &'t mut ArtMultiSlabAllocator<'t, V> {
        let (allocator_area, remain) = alloc_from_slice::<ArtMultiSlabAllocator<V>>(area);
        let (tree_area, remain) = alloc_from_slice::<Tree<V>>(remain);

        allocator_area.write(ArtMultiSlabAllocator {
            tree_area: spin::Mutex::new(Some(tree_area)),
            inner: MultiSlabAllocator::new(remain, &Self::LAYOUTS),
            phantom_val: PhantomData,
        })
    }
}

impl<'t, V: crate::Value> ArtAllocator<V> for ArtMultiSlabAllocator<'t, V> {
    fn alloc_tree(&self) -> *mut Tree<V> {
        let mut t = self.tree_area.lock();
        if let Some(tree_area) = t.take() {
            return tree_area.as_mut_ptr().cast();
        }
        panic!("cannot allocate more than one tree");
    }

    fn alloc_node_internal4(&self) -> *mut NodeInternal4<V> {
        self.inner.alloc_slab(0).cast()
    }
    fn alloc_node_internal16(&self) -> *mut NodeInternal16<V> {
        self.inner.alloc_slab(1).cast()
    }
    fn alloc_node_internal48(&self) -> *mut NodeInternal48<V> {
        self.inner.alloc_slab(2).cast()
    }
    fn alloc_node_internal256(&self) -> *mut NodeInternal256<V> {
        self.inner.alloc_slab(3).cast()
    }
    fn alloc_node_leaf(&self) -> *mut NodeLeaf<V> {
        self.inner.alloc_slab(4).cast()
    }

    fn dealloc_node_internal4(&self, ptr: *mut NodeInternal4<V>) {
        self.inner.dealloc_slab(0, ptr.cast())
    }

    fn dealloc_node_internal16(&self, ptr: *mut NodeInternal16<V>) {
        self.inner.dealloc_slab(1, ptr.cast())
    }
    fn dealloc_node_internal48(&self, ptr: *mut NodeInternal48<V>) {
        self.inner.dealloc_slab(2, ptr.cast())
    }
    fn dealloc_node_internal256(&self, ptr: *mut NodeInternal256<V>) {
        self.inner.dealloc_slab(3, ptr.cast())
    }
    fn dealloc_node_leaf(&self, ptr: *mut NodeLeaf<V>) {
        self.inner.dealloc_slab(4, ptr.cast())
    }
}

impl<'t, V: crate::Value> ArtMultiSlabAllocator<'t, V> {
    pub(crate) fn get_statistics(&self) -> ArtMultiSlabStats {
        ArtMultiSlabStats {
            num_internal4: self.inner.slab_descs[0]
                .num_allocated
                .load(Ordering::Relaxed),
            num_internal16: self.inner.slab_descs[1]
                .num_allocated
                .load(Ordering::Relaxed),
            num_internal48: self.inner.slab_descs[2]
                .num_allocated
                .load(Ordering::Relaxed),
            num_internal256: self.inner.slab_descs[3]
                .num_allocated
                .load(Ordering::Relaxed),
            num_leaf: self.inner.slab_descs[4]
                .num_allocated
                .load(Ordering::Relaxed),

            num_blocks_internal4: self.inner.slab_descs[0].num_blocks.load(Ordering::Relaxed),
            num_blocks_internal16: self.inner.slab_descs[1].num_blocks.load(Ordering::Relaxed),
            num_blocks_internal48: self.inner.slab_descs[2].num_blocks.load(Ordering::Relaxed),
            num_blocks_internal256: self.inner.slab_descs[3].num_blocks.load(Ordering::Relaxed),
            num_blocks_leaf: self.inner.slab_descs[4].num_blocks.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArtMultiSlabStats {
    pub num_internal4: u64,
    pub num_internal16: u64,
    pub num_internal48: u64,
    pub num_internal256: u64,
    pub num_leaf: u64,

    pub num_blocks_internal4: u64,
    pub num_blocks_internal16: u64,
    pub num_blocks_internal48: u64,
    pub num_blocks_internal256: u64,
    pub num_blocks_leaf: u64,
}
