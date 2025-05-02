mod block;
mod multislab;
mod slab;
mod r#static;

use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use crate::allocator::multislab::MultiSlabAllocator;

use crate::Tree;
pub use crate::algorithm::node_ptr::{
    NodeInternal4, NodeInternal16, NodeInternal48, NodeInternal256, NodeLeaf4, NodeLeaf16,
    NodeLeaf48, NodeLeaf256,
};

pub struct OutOfMemoryError();

pub trait ArtAllocator<V: crate::Value> {
    fn alloc_tree(&self) -> *mut Tree<V>;

    fn alloc_node_internal4(&self) -> *mut NodeInternal4<V>;
    fn alloc_node_internal16(&self) -> *mut NodeInternal16<V>;
    fn alloc_node_internal48(&self) -> *mut NodeInternal48<V>;
    fn alloc_node_internal256(&self) -> *mut NodeInternal256<V>;
    fn alloc_node_leaf4(&self) -> *mut NodeLeaf4<V>;
    fn alloc_node_leaf16(&self) -> *mut NodeLeaf16<V>;
    fn alloc_node_leaf48(&self) -> *mut NodeLeaf48<V>;
    fn alloc_node_leaf256(&self) -> *mut NodeLeaf256<V>;

    fn dealloc_node_internal4(&self, ptr: *mut NodeInternal4<V>);
    fn dealloc_node_internal16(&self, ptr: *mut NodeInternal16<V>);
    fn dealloc_node_internal48(&self, ptr: *mut NodeInternal48<V>);
    fn dealloc_node_internal256(&self, ptr: *mut NodeInternal256<V>);
    fn dealloc_node_leaf4(&self, ptr: *mut NodeLeaf4<V>);
    fn dealloc_node_leaf16(&self, ptr: *mut NodeLeaf16<V>);
    fn dealloc_node_leaf48(&self, ptr: *mut NodeLeaf48<V>);
    fn dealloc_node_leaf256(&self, ptr: *mut NodeLeaf256<V>);
}

#[repr(transparent)]
pub struct ArtMultiSlabAllocator<'t, V> {
    inner: MultiSlabAllocator<'t, 8>,

    phantom_val: PhantomData<V>,
}

impl<'t, V: crate::Value> ArtMultiSlabAllocator<'t, V> {
    const LAYOUTS: [Layout; 8] = [
        Layout::new::<NodeInternal4<V>>(),
        Layout::new::<NodeInternal16<V>>(),
        Layout::new::<NodeInternal48<V>>(),
        Layout::new::<NodeInternal256<V>>(),
        Layout::new::<NodeLeaf4<V>>(),
        Layout::new::<NodeLeaf16<V>>(),
        Layout::new::<NodeLeaf48<V>>(),
        Layout::new::<NodeLeaf256<V>>(),
    ];

    pub fn new(area: &'t mut [MaybeUninit<u8>]) -> &'t mut ArtMultiSlabAllocator<'t, V> {
        let allocator = MultiSlabAllocator::new(area, &Self::LAYOUTS);

        let ptr: *mut MultiSlabAllocator<8> = allocator;

        let ptr: *mut ArtMultiSlabAllocator<V> = ptr.cast();

        unsafe { ptr.as_mut().unwrap() }
    }
}

impl<'t, V: crate::Value> ArtAllocator<V> for ArtMultiSlabAllocator<'t, V> {
    fn alloc_tree(&self) -> *mut Tree<V> {
        self.inner.alloc_fit(Layout::new::<Tree<V>>()).cast()
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
    fn alloc_node_leaf4(&self) -> *mut NodeLeaf4<V> {
        self.inner.alloc_slab(4).cast()
    }
    fn alloc_node_leaf16(&self) -> *mut NodeLeaf16<V> {
        self.inner.alloc_slab(5).cast()
    }
    fn alloc_node_leaf48(&self) -> *mut NodeLeaf48<V> {
        self.inner.alloc_slab(6).cast()
    }
    fn alloc_node_leaf256(&self) -> *mut NodeLeaf256<V> {
        self.inner.alloc_slab(7).cast()
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
    fn dealloc_node_leaf4(&self, ptr: *mut NodeLeaf4<V>) {
        self.inner.dealloc_slab(4, ptr.cast())
    }
    fn dealloc_node_leaf16(&self, ptr: *mut NodeLeaf16<V>) {
        self.inner.dealloc_slab(5, ptr.cast())
    }
    fn dealloc_node_leaf48(&self, ptr: *mut NodeLeaf48<V>) {
        self.inner.dealloc_slab(6, ptr.cast())
    }
    fn dealloc_node_leaf256(&self, ptr: *mut NodeLeaf256<V>) {
        self.inner.dealloc_slab(7, ptr.cast())
    }
}
