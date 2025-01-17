use std::{
    ops::{Deref, Range, RangeBounds},
    sync::Arc,
};

use super::{alignment::Alignment, raw::RawAlignedBuffer, AlignedBufferMut, ConstAlign};

/// An shared, immutable aligned buffer type.
#[derive(Clone, Debug)]
pub struct AlignedBuffer<A: Alignment> {
    /// Shared raw buffer.
    raw: Arc<RawAlignedBuffer<A>>,
    /// Range that specifies the current slice.
    range: Range<usize>,
}

impl<A: Alignment> AlignedBuffer<A> {
    /// Creates an immutable `IoBuffer` from the raw buffer
    pub(super) fn from_raw(raw: RawAlignedBuffer<A>, range: Range<usize>) -> Self {
        AlignedBuffer {
            raw: Arc::new(raw),
            range,
        }
    }

    /// Returns the number of bytes in the buffer, also referred to as its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns the alignment of the buffer.
    #[inline]
    pub fn align(&self) -> usize {
        self.raw.align()
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        // SAFETY: `self.range.start` is guaranteed to be within [0, self.len()).
        unsafe { self.raw.as_ptr().add(self.range.start) }
    }

    /// Extracts a slice containing the entire buffer.
    ///
    /// Equivalent to `&s[..]`.
    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.raw.as_slice()[self.range.start..self.range.end]
    }

    /// Returns a slice of self for the index range `[begin..end)`.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        use core::ops::Bound;
        let len = self.len();

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("out of range"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        assert!(
            begin <= end,
            "range start must not be greater than end: {:?} <= {:?}",
            begin,
            end,
        );
        assert!(
            end <= len,
            "range end out of bounds: {:?} <= {:?}",
            end,
            len,
        );

        let begin = self.range.start + begin;
        let end = self.range.start + end;

        AlignedBuffer {
            raw: Arc::clone(&self.raw),
            range: begin..end,
        }
    }

    /// Returns the mutable aligned buffer, if the immutable aligned buffer
    /// has exactly one strong reference. Otherwise returns `None`.
    pub fn into_mut(self) -> Option<AlignedBufferMut<A>> {
        let raw = Arc::into_inner(self.raw)?;
        Some(AlignedBufferMut::from_raw(raw))
    }
}

impl<A: Alignment> Deref for AlignedBuffer<A> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<A: Alignment> AsRef<[u8]> for AlignedBuffer<A> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<A: Alignment> PartialEq<[u8]> for AlignedBuffer<A> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice().eq(other)
    }
}

impl<const A: usize, const N: usize> From<&[u8; N]> for AlignedBuffer<ConstAlign<A>> {
    fn from(value: &[u8; N]) -> Self {
        let mut buf = AlignedBufferMut::with_capacity(N);
        buf.extend_from_slice(value);
        buf.freeze()
    }
}

/// SAFETY: the underlying buffer references a stable memory region.
unsafe impl<A: Alignment> tokio_epoll_uring::IoBuf for AlignedBuffer<A> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.len()
    }
}
