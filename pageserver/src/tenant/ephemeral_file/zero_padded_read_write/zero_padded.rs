//! A [`crate::virtual_file::owned_buffers_io::write::Buffer`] whose
//! unwritten range is guaranteed to be zero-initialized.
//! This is used by [`crate::tenant::ephemeral_file::zero_padded_read_write::RW::read_blk`]
//! to serve page-sized reads of the trailing page when the trailing page has only been partially filled.

use std::mem::MaybeUninit;

use crate::virtual_file::owned_buffers_io::io_buf_ext::FullSlice;

/// See module-level comment.
pub struct Buffer<const N: usize> {
    allocation: Box<[u8; N]>,
    written: usize,
}

impl<const N: usize> Default for Buffer<N> {
    fn default() -> Self {
        Self {
            allocation: Box::new(
                // SAFETY: zeroed memory is a valid [u8; N]
                unsafe { MaybeUninit::zeroed().assume_init() },
            ),
            written: 0,
        }
    }
}

impl<const N: usize> Buffer<N> {
    #[inline(always)]
    fn invariants(&self) {
        // don't check by default, unoptimized is too expensive even for debug mode
        if false {
            debug_assert!(self.written <= N, "{}", self.written);
            debug_assert!(self.allocation[self.written..N].iter().all(|v| *v == 0));
        }
    }

    pub fn as_zero_padded_slice(&self) -> &[u8; N] {
        &self.allocation
    }
}

impl<const N: usize> crate::virtual_file::owned_buffers_io::write::Buffer for Buffer<N> {
    type IoBuf = Self;

    fn cap(&self) -> usize {
        self.allocation.len()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        self.invariants();
        let remaining = self.allocation.len() - self.written;
        if other.len() > remaining {
            panic!("calling extend_from_slice() with insufficient remaining capacity");
        }
        self.allocation[self.written..(self.written + other.len())].copy_from_slice(other);
        self.written += other.len();
        self.invariants();
    }

    fn pending(&self) -> usize {
        self.written
    }

    fn flush(self) -> FullSlice<Self> {
        self.invariants();
        let written = self.written;
        FullSlice::must_new(tokio_epoll_uring::BoundedBuf::slice(self, 0..written))
    }

    fn reuse_after_flush(iobuf: Self::IoBuf) -> Self {
        let Self {
            mut allocation,
            written,
        } = iobuf;
        allocation[0..written].fill(0);
        let new = Self {
            allocation,
            written: 0,
        };
        new.invariants();
        new
    }
}

/// We have this trait impl so that the `flush` method in the `Buffer` impl above can produce a
/// [`tokio_epoll_uring::BoundedBuf::slice`] of the [`Self::written`] range of the data.
///
/// Remember that bytes_init is generally _not_ a tracker of the amount
/// of valid data in the io buffer; we use `Slice` for that.
/// The `IoBuf` is _only_ for keeping track of uninitialized memory, a bit like MaybeUninit.
///
/// SAFETY:
///
/// The [`Self::allocation`] is stable becauses boxes are stable.
/// The memory is zero-initialized, so, bytes_init is always N.
unsafe impl<const N: usize> tokio_epoll_uring::IoBuf for Buffer<N> {
    fn stable_ptr(&self) -> *const u8 {
        self.allocation.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        // Yes, N, not self.written; Read the full comment of this impl block!
        N
    }

    fn bytes_total(&self) -> usize {
        N
    }
}
