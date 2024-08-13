use bytes::{Bytes, BytesMut};
use std::ops::Range;
use tokio_epoll_uring::{BoundedBuf, Slice};

pub(crate) trait IoBufExt {
    /// Get a [`Slice`] covering the range `[0..self.len()]`.
    /// It is guaranteed that the resulting slice has [`Slice::bytes_init`] equal to [`Slice::bytes_total`].
    fn slice_len(self) -> Slice<Self>
    where
        Self: Sized;
}

macro_rules! impl_io_buf_ext {
    ($($t:ty),*) => {
        $(
            impl IoBufExt for $t {
                #[inline(always)]
                fn slice_len(self) -> Slice<Self> {
                    let len = self.len();
                    let s = if len == 0 {
                        // paper over the incorrect assertion
                        // https://github.com/neondatabase/tokio-epoll-uring/issues/46
                        let slice = self.slice_full();
                        let mut bounds: Range<_> = slice.bounds();
                        bounds.end = bounds.start;
                        // from_buf_bounds has the correct assertion
                        Slice::from_buf_bounds(slice.into_inner(), bounds)
                    } else {
                        self.slice(0..len)
                    };
                    assert_eq!(s.bytes_init(), s.bytes_total());
                    s
                }
            }
        )*
    };
}

impl_io_buf_ext!(Bytes, BytesMut, Vec<u8>);
