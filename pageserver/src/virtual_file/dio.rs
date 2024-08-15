use std::ops::{Deref, DerefMut};

use aligned_vec::AVec;
use bytes::buf::UninitSlice;

pub const DIO_MEM_ALIGN: usize = 4096;

pub struct IoBufferMut(AVec<u8, aligned_vec::RuntimeAlign>);

impl IoBufferMut {
    // pub fn new(align: usize) -> Self {
    //     IoBufferMut(AVec::new(align))
    // }

    #[inline]
    pub fn with_capacity(align: usize, capacity: usize) -> Self {
        IoBufferMut(AVec::with_capacity(align, capacity))
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional)
    }

    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.0.set_len(new_len)
    }

    #[inline]
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        self.0.extend_from_slice(other)
    }

    /// Drops the all the elements of the vector, setting its length to `0`.
    #[inline]
    pub fn clear(&mut self) {
        self.0.clear()
    }
}

impl Deref for IoBufferMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for IoBufferMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

unsafe impl bytes::BufMut for IoBufferMut {
    #[inline]
    fn remaining_mut(&self) -> usize {
        // A vector can never have more than isize::MAX bytes
        core::isize::MAX as usize - self.len()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let len = self.len();
        let remaining = self.0.capacity() - len;

        if remaining < cnt {
            panic_advance(cnt, remaining);
        }

        // Addition will not overflow since the sum is at most the capacity.
        self.set_len(len + cnt);
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        if self.capacity() == self.len() {
            self.reserve(self.0.alignment()); // Grow the vec by alignment
        }

        let cap = self.capacity();
        let len = self.len();

        let ptr = self.as_mut_ptr();
        // SAFETY: Since `ptr` is valid for `cap` bytes, `ptr.add(len)` must be
        // valid for `cap - len` bytes. The subtraction will not underflow since
        // `len <= cap`.
        unsafe { UninitSlice::from_raw_parts_mut(ptr.add(len), cap - len) }
    }
}

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

/// Panic with a nice error message.
#[cold]
fn panic_advance(idx: usize, len: usize) -> ! {
    panic!(
        "advance out of bounds: the len is {} but advancing by {}",
        len, idx
    );
}
