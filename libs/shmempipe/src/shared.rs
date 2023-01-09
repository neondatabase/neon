//! Utilities shared across processes

use std::os::unix::prelude::FromRawFd;

pub struct EventfdSemaphore(std::os::unix::prelude::RawFd);

impl EventfdSemaphore {
    pub fn wait(&self) {
        let mut out = [0u8; 8];
        nix::unistd::read(self.0, &mut out).expect("reading from eventfd semaphore failed");
    }

    pub fn post(&self) {
        let one = 1u64.to_ne_bytes();
        nix::unistd::write(self.0, &one).expect("writing to eventfd semaphore failed");
    }
}

// FIXME: no drop impl for EventfdSemaphore, though we should have close when the drop actually happens.

impl FromRawFd for EventfdSemaphore {
    unsafe fn from_raw_fd(fd: std::os::unix::prelude::RawFd) -> Self {
        EventfdSemaphore(fd)
    }
}
