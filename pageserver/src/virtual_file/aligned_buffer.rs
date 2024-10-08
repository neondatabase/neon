#![allow(unused)]

use core::slice;
use std::{
    alloc::{self, Layout},
    cmp,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::{addr_of_mut, NonNull},
};

use bytes::buf::UninitSlice;

#[derive(Debug)]
struct IoBufferPtr(*mut u8);

// SAFETY: We gurantees no one besides `IoBufferPtr` itself has the raw pointer.
unsafe impl Send for IoBufferPtr {}

/// An aligned buffer type used for I/O.
#[derive(Debug)]
pub struct AlignedBufferMut<const ALIGN: usize> {
    ptr: IoBufferPtr,
    capacity: usize,
    len: usize,
}

pub struct AlignedSlice<'a, const ALIGN: usize, const N: usize>(&'a mut [u8; N]);

impl<'a, const ALIGN: usize, const N: usize> AlignedSlice<'a, ALIGN, N> {
    pub unsafe fn new_unchecked(buf: &'a mut [u8; N]) -> Self {
        AlignedSlice(buf)
    }
}

impl<'a, const ALIGN: usize, const N: usize> Deref for AlignedSlice<'a, ALIGN, N> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, const ALIGN: usize, const N: usize> DerefMut for AlignedSlice<'a, ALIGN, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'a, const ALIGN: usize, const N: usize> AsRef<[u8; N]> for AlignedSlice<'a, ALIGN, N> {
    fn as_ref(&self) -> &[u8; N] {
        &self.0
    }
}

impl<const ALIGN: usize> AlignedBufferMut<ALIGN> {
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
        let layout = Layout::from_size_align(capacity, ALIGN).expect("Invalid layout");

        // SAFETY:  Making an allocation with a sized and aligned layout. The memory is manually freed with the same layout.
        let ptr = unsafe {
            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            IoBufferPtr(ptr)
        };

        AlignedBufferMut {
            ptr,
            capacity,
            len: 0,
        }
    }

    /// Constructs a new `IoBufferMut` with at least the specified capacity and alignment, filled with zeros.
    pub fn with_capacity_zeroed(capacity: usize) -> Self {
        use bytes::BufMut;
        let mut buf = Self::with_capacity(capacity);
        buf.put_bytes(0, capacity);
        buf.len = capacity;
        buf
    }

    /// Returns the total number of bytes the buffer can hold.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the alignment of the buffer.
    #[inline]
    pub const fn align(&self) -> usize {
        ALIGN
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
        // SAFETY: The pointer is valid and `len` bytes are initialized.
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    /// Extracts a mutable slice of the entire buffer.
    ///
    /// Equivalent to `&mut s[..]`.
    fn as_mut_slice(&mut self) -> &mut [u8] {
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
            (IoBufferPtr(ptr), cap)
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

impl<const ALIGN: usize> Drop for AlignedBufferMut<ALIGN> {
    fn drop(&mut self) {
        // SAFETY: memory was allocated with std::alloc::alloc with the same layout.
        unsafe {
            alloc::dealloc(
                self.as_mut_ptr(),
                Layout::from_size_align_unchecked(self.capacity, ALIGN),
            )
        }
    }
}

impl<const ALIGN: usize> Deref for AlignedBufferMut<ALIGN> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<const ALIGN: usize> DerefMut for AlignedBufferMut<ALIGN> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<const ALIGN: usize> AsRef<[u8]> for AlignedBufferMut<ALIGN> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const ALIGN: usize> AsMut<[u8]> for AlignedBufferMut<ALIGN> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl<const ALIGN: usize> PartialEq<[u8]> for AlignedBufferMut<ALIGN> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice().eq(other)
    }
}

/// SAFETY: When advancing the internal cursor, the caller needs to make sure the bytes advcanced past have been initialized.
unsafe impl<const ALIGN: usize> bytes::BufMut for AlignedBufferMut<ALIGN> {
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
unsafe impl<const ALIGN: usize> tokio_epoll_uring::IoBuf for AlignedBufferMut<ALIGN> {
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
unsafe impl<const ALIGN: usize> tokio_epoll_uring::IoBufMut for AlignedBufferMut<ALIGN> {
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

    const ALIGN: usize = 4 * 1024;
    type TestIoBufferMut = AlignedBufferMut<ALIGN>;

    #[test]
    fn test_with_capacity() {
        let v = TestIoBufferMut::with_capacity(ALIGN * 4);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN * 4);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);

        let v = TestIoBufferMut::with_capacity(ALIGN / 2);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), ALIGN / 2);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
    }

    #[test]
    fn test_with_capacity_zeroed() {
        let v = TestIoBufferMut::with_capacity_zeroed(ALIGN);
        assert_eq!(v.len(), ALIGN);
        assert_eq!(v.capacity(), ALIGN);
        assert_eq!(v.align(), ALIGN);
        assert_eq!(v.as_ptr().align_offset(ALIGN), 0);
        assert_eq!(&v[..], &[0; ALIGN])
    }

    #[test]
    fn test_reserve() {
        use bytes::BufMut;
        let mut v = TestIoBufferMut::with_capacity(ALIGN);
        let capacity = v.capacity();
        v.reserve(capacity);
        assert_eq!(v.capacity(), capacity);
        let data = [b'a'; ALIGN];
        v.put(&data[..]);
        v.reserve(capacity);
        assert!(v.capacity() >= capacity * 2);
        assert_eq!(&v[..], &data[..]);
        let capacity = v.capacity();
        v.clear();
        v.reserve(capacity);
        assert_eq!(capacity, v.capacity());
    }

    #[test]
    fn test_bytes_put() {
        use bytes::BufMut;
        let mut v = TestIoBufferMut::with_capacity(ALIGN * 4);
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
        let mut v = TestIoBufferMut::with_capacity(ALIGN * 4);
        let x = [b'a'; ALIGN];
        for _ in 0..5 {
            v.put_slice(&x[..]);
        }
    }

    #[test]
    fn test_io_buf_put_slice() {
        use tokio_epoll_uring::BoundedBufMut;
        const ALIGN: usize = 4 * 1024;
        let mut v = TestIoBufferMut::with_capacity(ALIGN);
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
