#![allow(unused)]

use core::slice;
use std::{
    alloc::{self, Layout},
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use bytes::buf::UninitSlice;

struct IoBufferPtr(*mut u8);

unsafe impl Send for IoBufferPtr {}

/// An aligned buffer type used for I/O.
pub struct IoBufferMut {
    ptr: IoBufferPtr,
    capacity: usize,
    len: usize,
    align: usize,
}

impl IoBufferMut {
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
    ///
    /// # Examples
    ///
    /// ```
    /// let mut buf = IoBufferMut::with_capacity_aligned(4096, 4096);
    ///
    /// assert_eq!(buf.len(), 0);
    /// assert_eq!(buf.capacity(), 4096);
    /// assert_eq!(buf.align(), 4096);
    /// ```
    pub fn with_capacity_aligned(capacity: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(capacity, align).expect("Invalid layout");

        // SAFETY:  Making an allocation with a sized and aligned layout. The memory is manually freed with the same layout.
        let ptr = unsafe {
            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            IoBufferPtr(ptr)
        };

        IoBufferMut {
            ptr,
            capacity,
            len: 0,
            align,
        }
    }

    /// Returns the total number of bytes the buffer can hold.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the alignment of the buffer.
    #[inline]
    pub fn align(&self) -> usize {
        self.align
    }

    /// Returns the number of bytes in the buffer, also referred to as its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Force the length of the buffer to `new_len`.
    #[inline]
    unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity());
        self.len = new_len;
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        self.ptr.0
    }

    #[inline]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.0
    }

    /// Extracts a slice containing the entire buffer.
    ///
    /// Equivalent to `&s[..]`.
    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    /// Extracts a mutable slice of the entire buffer.
    ///
    /// Equivalent to `&mut s[..]`.
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    /// Drops the all the contents of the buffer, setting its length to `0`.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl Drop for IoBufferMut {
    fn drop(&mut self) {
        // SAFETY: memory was allocated with std::alloc::alloc with the same layout.
        unsafe {
            alloc::dealloc(
                self.as_mut_ptr(),
                Layout::from_size_align_unchecked(self.capacity, self.align),
            )
        }
    }
}

impl Deref for IoBufferMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for IoBufferMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// SAFETY: See [`IoBufferMut::advance_mut`]
unsafe impl bytes::BufMut for IoBufferMut {
    #[inline]
    fn remaining_mut(&self) -> usize {
        // Although a `Vec` can have at most isize::MAX bytes, we never want to grow `IoBufferMut`.
        // Thus, it can have at most `self.capacity` bytes.
        self.capacity() - self.len()
    }

    // SAFETY: Caller needs to make sure the bytes being advanced past have been initialized.
    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let len = self.len();
        let remaining = self.remaining_mut();

        if remaining < cnt {
            panic_advance(cnt, remaining);
        }

        // Addition will not overflow since the sum is at most the capacity.
        self.set_len(len + cnt);
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        let cap = self.capacity();
        let len = self.len();

        // SAFETY: Since `self.ptr` is valid for `cap` bytes, `self.ptr.add(len)` must be
        // valid for `cap - len` bytes. The subtraction will not underflow since
        // `len <= cap`.
        unsafe { UninitSlice::from_raw_parts_mut(self.as_mut_ptr().add(len), cap - len) }
    }
}

/// Panic with a nice error message.
#[cold]
fn panic_advance(idx: usize, len: usize) -> ! {
    panic!(
        "advance out of bounds: the len is {} but advancing by {}",
        len, idx
    );
}

/// Safety: [`IoBufferMut`] has exclusive ownership of the io buffer,
/// and the location remains stable even if [`Self`] is moved.
unsafe impl tokio_epoll_uring::IoBuf for IoBufferMut {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: See above.
unsafe impl tokio_epoll_uring::IoBufMut for IoBufferMut {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        if self.len() < init_len {
            self.set_len(init_len);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_with_capacity_aligned() {
        const ALIGN: usize = 4 * 1024;
        let v = IoBufferMut::with_capacity_aligned(ALIGN * 4, ALIGN);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN * 4);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);

        let v = IoBufferMut::with_capacity_aligned(ALIGN / 2, ALIGN);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN / 2);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }

    #[test]
    fn test_bytes_put() {
        use bytes::BufMut;
        const ALIGN: usize = 4 * 1024;
        let mut v = IoBufferMut::with_capacity_aligned(ALIGN * 4, ALIGN);
        let x = [b'a'; ALIGN];

        for _ in 0..2 {
            for _ in 0..4 {
                v.put(&x[..]);
            }
            assert_eq!(v.len(), ALIGN * 4);
            assert_eq!(v.capacity(), ALIGN * 4);
            assert_eq!(v.align(), ALIGN);
            assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
            v.clear()
        }
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN * 4);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }

    #[test]
    #[should_panic]
    fn test_bytes_put_panic() {
        use bytes::BufMut;
        const ALIGN: usize = 4 * 1024;
        let mut v = IoBufferMut::with_capacity_aligned(ALIGN * 4, ALIGN);
        let x = [b'a'; ALIGN];
        for _ in 0..5 {
            v.put_slice(&x[..]);
        }
    }

    #[test]
    fn test_io_buf_put_slice() {
        use tokio_epoll_uring::BoundedBufMut;
        const ALIGN: usize = 4 * 1024;
        let mut v = IoBufferMut::with_capacity_aligned(ALIGN, ALIGN);
        let x = [b'a'; ALIGN];

        for _ in 0..2 {
            v.put_slice(&x[..]);
            assert_eq!(v.len(), ALIGN);
            assert_eq!(v.capacity(), ALIGN);
            assert_eq!(v.align(), ALIGN);
            assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
            v.clear()
        }
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }
}
