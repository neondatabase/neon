//! See [`FullSlice`].

use crate::virtual_file::{IoBuffer, IoBufferMut};
use bytes::{Bytes, BytesMut};
use std::ops::{Deref, Range};
use tokio_epoll_uring::{BoundedBuf, IoBuf, Slice};

use super::write::CheapCloneForRead;

/// The true owned equivalent for Rust [`slice`]. Use this for the write path.
///
/// Unlike [`tokio_epoll_uring::Slice`], which we unfortunately inherited from `tokio-uring`,
/// [`FullSlice`] is guaranteed to have all its bytes initialized. This means that
/// [`<FullSlice as Deref<Target = [u8]>>::len`] is equal to [`Slice::bytes_init`] and [`Slice::bytes_total`].
///
pub struct FullSlice<B> {
    slice: Slice<B>,
}

impl<B> FullSlice<B>
where
    B: IoBuf,
{
    pub(crate) fn must_new(slice: Slice<B>) -> Self {
        assert_eq!(slice.bytes_init(), slice.bytes_total());
        FullSlice { slice }
    }
    pub(crate) fn into_raw_slice(self) -> Slice<B> {
        let FullSlice { slice: s } = self;
        s
    }
}

impl<B> Deref for FullSlice<B>
where
    B: IoBuf,
{
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        let rust_slice = &self.slice[..];
        assert_eq!(rust_slice.len(), self.slice.bytes_init());
        assert_eq!(rust_slice.len(), self.slice.bytes_total());
        rust_slice
    }
}

impl<B> CheapCloneForRead for FullSlice<B>
where
    B: IoBuf + CheapCloneForRead,
{
    fn cheap_clone(&self) -> Self {
        let bounds = self.slice.bounds();
        let clone = self.slice.get_ref().cheap_clone();
        let slice = clone.slice(bounds);
        Self { slice }
    }
}

pub(crate) trait IoBufExt {
    /// Get a [`FullSlice`] for the entire buffer, i.e., `self[..]` or `self[0..self.len()]`.
    fn slice_len(self) -> FullSlice<Self>
    where
        Self: Sized;
}

macro_rules! impl_io_buf_ext {
    ($T:ty) => {
        impl IoBufExt for $T {
            #[inline(always)]
            fn slice_len(self) -> FullSlice<Self> {
                let len = self.len();
                let s = if len == 0 {
                    // `BoundedBuf::slice(0..len)` or `BoundedBuf::slice(..)` has an incorrect assertion,
                    // causing a panic if len == 0.
                    // The Slice::from_buf_bounds has the correct assertion (<= instead of <).
                    // => https://github.com/neondatabase/tokio-epoll-uring/issues/46
                    let slice = self.slice_full();
                    let mut bounds: Range<_> = slice.bounds();
                    bounds.end = bounds.start;
                    Slice::from_buf_bounds(slice.into_inner(), bounds)
                } else {
                    self.slice(0..len)
                };
                FullSlice::must_new(s)
            }
        }
    };
}

impl_io_buf_ext!(Bytes);
impl_io_buf_ext!(BytesMut);
impl_io_buf_ext!(Vec<u8>);
impl_io_buf_ext!(IoBufferMut);
impl_io_buf_ext!(IoBuffer);
