use std::cell::RefCell;

use postgres_ffi::BLCKSZ;

pub struct PooledVecU8(Option<Vec<u8>>);

// Thread-local list of re-usable buffers.
thread_local! {
    static POOL: RefCell<Vec<Vec<u8>>> = RefCell::new(Vec::new());
}

pub(crate) fn get() -> PooledVecU8 {
    let maybe = POOL.with(|rc| rc.borrow_mut().pop());
    match maybe {
        Some(buf) => PooledVecU8(Some(buf)),
        None => PooledVecU8(Some(Vec::with_capacity((BLCKSZ as usize) * 3))),
    }
}

impl Drop for PooledVecU8 {
    fn drop(&mut self) {
        let mut buf = self.0.take().unwrap();
        buf.clear();
        POOL.with(|rc| rc.borrow_mut().push(buf))
    }
}

impl std::ops::Deref for PooledVecU8 {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledVecU8 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}
