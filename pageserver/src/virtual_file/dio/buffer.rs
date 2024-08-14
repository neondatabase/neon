use std::ops::{Deref, DerefMut};

use aligned_vec::AVec;
use bytes::buf::UninitSlice;

pub struct IoBufferMut(AVec<u8, aligned_vec::RuntimeAlign>);

impl IoBufferMut {
    pub fn new(align: usize) -> Self {
        IoBufferMut(AVec::new(align))
    }

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

    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.0.set_len(new_len)
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

/// Panic with a nice error message.
#[cold]
fn panic_advance(idx: usize, len: usize) -> ! {
    panic!(
        "advance out of bounds: the len is {} but advancing by {}",
        len, idx
    );
}

// #[cold]
// fn panic_does_not_fit(size: usize, nbytes: usize) -> ! {
//     panic!(
//         "size too large: the integer type can fit {} bytes, but nbytes is {}",
//         size, nbytes
//     );
// }
