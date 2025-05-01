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

/*
pub struct Allocator {
    area: *mut MaybeUninit<u8>,
    allocated: AtomicUsize,
    size: usize,
}

const MAXALIGN: usize = std::mem::align_of::<usize>();

impl Allocator {
    pub fn new_uninit(area: &'static mut [MaybeUninit<u8>]) -> Allocator {
        let ptr = area.as_mut_ptr();
        let size = area.len();
        Self::new_from_ptr(ptr, size)
    }

    pub fn new(area: &'static mut [u8]) -> Allocator {
        let ptr: *mut MaybeUninit<u8> = area.as_mut_ptr().cast();
        let size = area.len();
        Self::new_from_ptr(ptr, size)
    }

    pub fn new_from_ptr(ptr: *mut MaybeUninit<u8>, size: usize) -> Allocator {
        let padding = ptr.align_offset(MAXALIGN);

        Allocator {
            area: ptr,
            allocated: AtomicUsize::new(padding),
            size,
        }
    }

    pub fn alloc<'a, T: Sized>(&'a self, value: T) -> AllocatedBox<'a, T> {
        let sz = std::mem::size_of::<T>();

        // pad all allocations to MAXALIGN boundaries
        assert!(std::mem::align_of::<T>() <= MAXALIGN);
        let sz = sz.next_multiple_of(MAXALIGN);

        let offset = self.allocated.fetch_add(sz, Ordering::Relaxed);

        if offset + sz > self.size {
            panic!("out of memory");
        }

        let inner = unsafe {
            let inner = self.area.offset(offset as isize).cast::<T>();
            *inner = value;
            NonNull::new_unchecked(inner)
        };

        AllocatedBox {
            inner,
            _phantom: PhantomData,
        }
    }

    pub fn _dealloc_node<T>(&self, _node: AllocatedBox<T>) {
        // doesn't free it immediately.
    }
}
*/
