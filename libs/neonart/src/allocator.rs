use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Allocator {
    area: *mut MaybeUninit<u8>,
    allocated: AtomicUsize,
    size: usize,
}

// FIXME: I don't know if these are really safe...
unsafe impl Send for Allocator {}
unsafe impl Sync for Allocator {}

#[repr(transparent)]
pub struct AllocatedBox<'a, T> {
    inner: NonNull<T>,

    _phantom: PhantomData<&'a Allocator>,
}

// FIXME: I don't know if these are really safe...
unsafe impl<'a, T> Send for AllocatedBox<'a, T> {}
unsafe impl<'a, T> Sync for AllocatedBox<'a, T> {}

impl<T> Deref for AllocatedBox<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { self.inner.as_ref() }
    }
}

impl<T> DerefMut for AllocatedBox<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { self.inner.as_mut() }
    }
}

impl<T> AsMut<T> for AllocatedBox<'_, T> {
    fn as_mut(&mut self) -> &mut T {
        unsafe { self.inner.as_mut() }
    }
}

impl<T> AllocatedBox<'_, T> {
    pub fn as_ptr(&self) -> *mut T {
        self.inner.as_ptr()
    }
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
