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
        debug_assert!(self.allocation[self.written..N].iter().all(|v| *v == 0));
    }

    pub fn as_zero_padded_slice(&self) -> &[u8; N] {
        &self.allocation
    }
}

/// SAFETY:
///
/// The [`Self::allocation`] is stable becauses boxes are stable.
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

/// SAFETY:
///
/// The [`Self::allocation`] is stable becauses boxes are stable.
///
unsafe impl<const N: usize> tokio_epoll_uring::IoBufMut for Buf<N> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.allocation.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.invariants();
        if pos < self.written {
            self.allocation[pos..self.written].fill(0);
        }
        self.written = pos;
        self.invariants();
    }
}
