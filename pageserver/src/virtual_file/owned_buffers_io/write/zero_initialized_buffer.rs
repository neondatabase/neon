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
        debug_assert!(self.written <= N, "{}", self.written);
    }

    pub fn as_zero_padded_slice(&self) -> &[u8; N] {
        &self.allocation
    }

    /// panics if there's not enough capacity left
    pub fn extend_from_slice(&mut self, buf: &[u8]) {
        self.invariants();
        let can = N - self.written;
        let want = buf.len();
        assert!(want <= can, "{:x} {:x}", want, can);
        self.allocation[self.written..(self.written + want)].copy_from_slice(buf);
        self.written += want;
        self.invariants();
    }

    pub fn len(&self) -> usize {
        self.written
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        self.invariants();
        self.written = 0;
        self.allocation[..].fill(0);
        self.invariants();
    }
}

/// SAFETY: the Box<> has a stable location in memory.
unsafe impl<const N: usize> tokio_epoll_uring::IoBuf for Buf<N> {
    fn stable_ptr(&self) -> *const u8 {
        self.allocation.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.written
    }

    fn bytes_total(&self) -> usize {
        self.written // ?
    }
}
