use std::ops::{Deref, DerefMut};

use crate::page_cache::PAGE_SZ;

pub struct BufferPool;

const POOL: BufferPool = BufferPool;

pub fn get() -> &'static BufferPool {
    &POOL
}

impl BufferPool {
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
