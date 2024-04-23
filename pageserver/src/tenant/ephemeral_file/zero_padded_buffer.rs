use std::mem::MaybeUninit;

pub struct Buf<const N: usize> {
    allocation: Box<[u8; N]>,
    written: usize,
}

impl<const N: usize> Default for Buf<N> {
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

impl<const N: usize> Buf<N> {
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

impl<const N: usize> crate::virtual_file::owned_buffers_io::write::Buffer for Buf<N> {
    type IoBuf = Self;

    fn cap(&self) -> usize {
        self.allocation.len()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        self.invariants();
        let remaining = self.cap() - other.len();
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

    fn flush(self) -> tokio_epoll_uring::Slice<Self> {
        self.invariants();
        let written = self.written;
        tokio_epoll_uring::BoundedBuf::slice(self, 0..written)
    }

    fn reconstruct_after_flush(this: Self::IoBuf) -> Self {
        let Self {
            mut allocation,
            written,
        } = this;
        allocation[0..written].fill(0);
        let new = Self {
            allocation,
            written: 0,
        };
        new.invariants();
        new
    }
}

/// We have this implementation so that Buf<N> can be used with [`crate::virtual_file::owned_buffers_io::BufferedWriter`].
///
///
/// SAFETY:
///
/// The [`Self::allocation`] is stable becauses boxes are stable.
/// The memory is zero-initialized, so, bytes_init is always N.
///
unsafe impl<const N: usize> tokio_epoll_uring::IoBuf for Buf<N> {
    fn stable_ptr(&self) -> *const u8 {
        self.allocation.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        N
    }

    fn bytes_total(&self) -> usize {
        N
    }
}
