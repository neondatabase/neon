use tokio_epoll_uring::BoundedBuf;
use tokio_epoll_uring::BoundedBufMut;
use tokio_epoll_uring::IoBufMut;
use tokio_epoll_uring::Slice;

pub(crate) trait SliceMutExt {
    /// Get a `&mut[0..self.bytes_total()`] slice, for when you need to do borrow-based IO.
    ///
    /// See the test case `test_slice_full_zeroed` for the difference to just doing `&slice[..]`
    fn as_mut_rust_slice_full_zeroed(&mut self) -> &mut [u8];
}

impl<B> SliceMutExt for Slice<B>
where
    B: IoBufMut,
{
    #[inline(always)]
    fn as_mut_rust_slice_full_zeroed(&mut self) -> &mut [u8] {
        // zero-initialize the uninitialized parts of the buffer so we can create a Rust slice
        //
        // SAFETY: we own `slice`, don't write outside the bounds
        unsafe {
            let to_init = self.bytes_total() - self.bytes_init();
            self.stable_mut_ptr()
                .add(self.bytes_init())
                .write_bytes(0, to_init);
            self.set_init(self.bytes_total());
        };
        let bytes_total = self.bytes_total();
        &mut self[0..bytes_total]
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use bytes::Buf;
    use tokio_epoll_uring::Slice;

    #[test]
    fn test_slice_full_zeroed() {
        let make_fake_file = || bytes::BytesMut::from(&b"12345"[..]).reader();

        // before we start the test, let's make sure we have a shared understanding of what slice_full does
        {
            let buf = Vec::with_capacity(3);
            let slice: Slice<_> = buf.slice_full();
            assert_eq!(slice.bytes_init(), 0);
            assert_eq!(slice.bytes_total(), 3);
            let rust_slice = &slice[..];
            assert_eq!(
                rust_slice.len(),
                0,
                "Slice only derefs to a &[u8] of the initialized part"
            );
        }

        // and also let's establish a shared understanding of .slice()
        {
            let buf = Vec::with_capacity(3);
            let slice: Slice<_> = buf.slice(0..2);
            assert_eq!(slice.bytes_init(), 0);
            assert_eq!(slice.bytes_total(), 2);
            let rust_slice = &slice[..];
            assert_eq!(
                rust_slice.len(),
                0,
                "Slice only derefs to a &[u8] of the initialized part"
            );
        }

        // the above leads to the easy mistake of using slice[..] for borrow-based IO like so:
        {
            let buf = Vec::with_capacity(3);
            let mut slice: Slice<_> = buf.slice_full();
            assert_eq!(slice[..].len(), 0);
            let mut file = make_fake_file();
            file.read_exact(&mut slice[..]).unwrap(); // one might think this reads 3 bytes but it reads 0
            assert_eq!(&slice[..] as &[u8], &[][..] as &[u8]);
        }

        // With owned buffers IO like with VirtualFilem, you could totally
        // pass in a `Slice` with bytes_init()=0 but bytes_total()=5
        // and it will read 5 bytes into the slice, and return a slice that has bytes_init()=5.
        {
            // TODO: demo
        }

        //
        // Ok, now that we have a shared understanding let's demo how to use the extension trait.
        //

        // slice_full()
        {
            let buf = Vec::with_capacity(3);
            let mut slice: Slice<_> = buf.slice_full();
            let rust_slice = slice.as_mut_rust_slice_full_zeroed();
            assert_eq!(rust_slice.len(), 3);
            assert_eq!(rust_slice, &[0, 0, 0]);
            let mut file = make_fake_file();
            file.read_exact(rust_slice).unwrap();
            assert_eq!(rust_slice, b"123");
            assert_eq!(&slice[..], b"123");
        }

        // .slice(..)
        {
            let buf = Vec::with_capacity(3);
            let mut slice: Slice<_> = buf.slice(0..2);
            let rust_slice = slice.as_mut_rust_slice_full_zeroed();
            assert_eq!(rust_slice.len(), 2);
            assert_eq!(rust_slice, &[0, 0]);
            let mut file = make_fake_file();
            file.read_exact(rust_slice).unwrap();
            assert_eq!(rust_slice, b"12");
            assert_eq!(&slice[..], b"12");
        }
    }
}
