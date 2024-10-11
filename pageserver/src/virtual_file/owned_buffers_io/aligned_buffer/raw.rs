use core::slice;
use std::{
    alloc::{self, Layout},
    cmp,
    mem::ManuallyDrop,
};

use super::alignment::{Alignment, ConstAlign};

#[derive(Debug)]
struct AlignedBufferPtr(*mut u8);

// SAFETY: We gurantees no one besides `IoBufferPtr` itself has the raw pointer.
unsafe impl Send for AlignedBufferPtr {}

// SAFETY: We gurantees no one besides `IoBufferPtr` itself has the raw pointer.
unsafe impl Sync for AlignedBufferPtr {}

/// An aligned buffer type.
#[derive(Debug)]
pub struct RawAlignedBuffer<A: Alignment> {
    ptr: AlignedBufferPtr,
    capacity: usize,
    len: usize,
    align: A,
}

impl<const A: usize> RawAlignedBuffer<ConstAlign<A>> {
    /// Constructs a new, empty `IoBufferMut` with at least the specified capacity and alignment.
    ///
    /// The buffer will be able to hold at most `capacity` elements and will never resize.
    ///
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` _bytes_, or if the following alignment requirement is not met:
    /// * `align` must not be zero,
    ///
    /// * `align` must be a power of two,
    ///
    /// * `capacity`, when rounded up to the nearest multiple of `align`,
    ///    must not overflow isize (i.e., the rounded value must be
    ///    less than or equal to `isize::MAX`).
    pub fn with_capacity(capacity: usize) -> Self {
        let align = ConstAlign::<A>;
        let layout = Layout::from_size_align(capacity, align.align()).expect("Invalid layout");

        // SAFETY:  Making an allocation with a sized and aligned layout. The memory is manually freed with the same layout.
        let ptr = unsafe {
            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            AlignedBufferPtr(ptr)
        };

        RawAlignedBuffer {
            ptr,
            capacity,
            len: 0,
            align,
        }
    }
}

impl<A: Alignment> RawAlignedBuffer<A> {
    /// Returns the total number of bytes the buffer can hold.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the alignment of the buffer.
    #[inline]
    pub fn align(&self) -> usize {
        self.align.align()
    }

    /// Returns the number of bytes in the buffer, also referred to as its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Force the length of the buffer to `new_len`.
    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity());
        self.len = new_len;
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.0
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.0
    }

    /// Extracts a slice containing the entire buffer.
    ///
    /// Equivalent to `&s[..]`.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: The pointer is valid and `len` bytes are initialized.
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    /// Extracts a mutable slice of the entire buffer.
    ///
    /// Equivalent to `&mut s[..]`.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: The pointer is valid and `len` bytes are initialized.
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    /// Drops the all the contents of the buffer, setting its length to `0`.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Reserves capacity for at least `additional` more bytes to be inserted
    /// in the given `IoBufferMut`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` _bytes_.
    pub fn reserve(&mut self, additional: usize) {
        if additional > self.capacity() - self.len() {
            self.reserve_inner(additional);
        }
    }

    fn reserve_inner(&mut self, additional: usize) {
        let Some(required_cap) = self.len().checked_add(additional) else {
            capacity_overflow()
        };

        let old_capacity = self.capacity();
        let align = self.align();
        // This guarantees exponential growth. The doubling cannot overflow
        // because `cap <= isize::MAX` and the type of `cap` is `usize`.
        let cap = cmp::max(old_capacity * 2, required_cap);

        if !is_valid_alloc(cap) {
            capacity_overflow()
        }
        let new_layout = Layout::from_size_align(cap, self.align()).expect("Invalid layout");

        let old_ptr = self.as_mut_ptr();

        // SAFETY: old allocation was allocated with std::alloc::alloc with the same layout,
        // and we panics on null pointer.
        let (ptr, cap) = unsafe {
            let old_layout = Layout::from_size_align_unchecked(old_capacity, align);
            let ptr = alloc::realloc(old_ptr, old_layout, new_layout.size());
            if ptr.is_null() {
                alloc::handle_alloc_error(new_layout);
            }
            (AlignedBufferPtr(ptr), cap)
        };

        self.ptr = ptr;
        self.capacity = cap;
    }

    /// Shortens the buffer, keeping the first len bytes.
    pub fn truncate(&mut self, len: usize) {
        if len > self.len {
            return;
        }
        self.len = len;
    }

    /// Consumes and leaks the `IoBufferMut`, returning a mutable reference to the contents, &'a mut [u8].
    pub fn leak<'a>(self) -> &'a mut [u8] {
        let mut buf = ManuallyDrop::new(self);
        // SAFETY: leaking the buffer as intended.
        unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.len) }
    }
}

fn capacity_overflow() -> ! {
    panic!("capacity overflow")
}

// We need to guarantee the following:
// * We don't ever allocate `> isize::MAX` byte-size objects.
// * We don't overflow `usize::MAX` and actually allocate too little.
//
// On 64-bit we just need to check for overflow since trying to allocate
// `> isize::MAX` bytes will surely fail. On 32-bit and 16-bit we need to add
// an extra guard for this in case we're running on a platform which can use
// all 4GB in user-space, e.g., PAE or x32.
#[inline]
fn is_valid_alloc(alloc_size: usize) -> bool {
    !(usize::BITS < 64 && alloc_size > isize::MAX as usize)
}

impl<A: Alignment> Drop for RawAlignedBuffer<A> {
    fn drop(&mut self) {
        // SAFETY: memory was allocated with std::alloc::alloc with the same layout.
        unsafe {
            alloc::dealloc(
                self.as_mut_ptr(),
                Layout::from_size_align_unchecked(self.capacity, self.align.align()),
            )
        }
    }
}
