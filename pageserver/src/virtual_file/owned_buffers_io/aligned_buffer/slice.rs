use std::ops::{Deref, DerefMut};

use super::alignment::{Alignment, ConstAlign};

/// Newtype for an aligned slice.
pub struct AlignedSlice<'a, const N: usize, A: Alignment> {
    /// underlying byte slice
    buf: &'a mut [u8; N],
    /// alignment marker
    _align: A,
}

impl<'a, const N: usize, const A: usize> AlignedSlice<'a, N, ConstAlign<A>> {
    /// Create a new aligned slice from a mutable byte slice. The input must already satisify the alignment.
    pub unsafe fn new_unchecked(buf: &'a mut [u8; N]) -> Self {
        let _align = ConstAlign::<A>;
        assert_eq!(buf.as_ptr().align_offset(_align.align()), 0);
        AlignedSlice { buf, _align }
    }
}

impl<const N: usize, A: Alignment> Deref for AlignedSlice<'_, N, A> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl<const N: usize, A: Alignment> DerefMut for AlignedSlice<'_, N, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf
    }
}

impl<const N: usize, A: Alignment> AsRef<[u8; N]> for AlignedSlice<'_, N, A> {
    fn as_ref(&self) -> &[u8; N] {
        self.buf
    }
}
