//! Buffer pool for ephemeral file buffers.
//!
//! Currently this is a very simple implementation that just uses `malloc`.
//! But the interface is such that we can switch to a more sophisticated
//! implementation later, e.g., one that caps that amount of memory used.

use std::ops::{Deref, DerefMut};

use crate::page_cache::PAGE_SZ;

pub struct BufferPool;

const POOL: BufferPool = BufferPool;

pub(super) fn get() -> &'static BufferPool {
    &POOL
}

impl BufferPool {
    /// Get a [`Handle`] to a buffer in the pool.
    ///
    /// The buffer is guaranteed to be zeroed out.
    ///
    /// The implementation may block to wait for buffers to become available,
    /// and a future async version of this method may `.await` internally to
    /// wait for buffers to become available.
    ///
    /// To avoid deadlocks, a thread/task must get all the buffers it needs
    /// with a single call to `get_buffer`. Without this rule, a deadlock
    /// can happen. Take for example a buffer pool with 2 buffers X, Y
    /// and a program with two threads A and B, each requiring 2 buffers.
    /// If A gets X and B gets Y, then both threads will block forever trying
    /// to get their second buffer.
    pub fn get_buffer(&self) -> Handle {
        Handle {
            data: vec![0; PAGE_SZ],
        }
    }
}

pub struct Handle {
    data: Vec<u8>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("data", &self.data.as_ptr())
            .finish()
    }
}

impl Deref for Handle {
    type Target = [u8; PAGE_SZ];
    fn deref(&self) -> &Self::Target {
        let slice: &[u8] = &self.data[..];
        slice.try_into().unwrap()
    }
}

impl DerefMut for Handle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let slice: &mut [u8] = &mut self.data[..];
        slice.try_into().unwrap()
    }
}
