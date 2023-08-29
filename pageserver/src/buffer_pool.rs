use std::cell::RefCell;

use crate::tenant::disk_btree::PAGE_SZ;

pub struct Buffer(Option<Box<[u8; PAGE_SZ]>>);

// Thread-local list of re-usable buffers.
thread_local! {
    static POOL: RefCell<Vec<Box<[u8; PAGE_SZ]>>> = RefCell::new(Vec::new());
}

pub(crate) fn get() -> Buffer {
    let maybe = POOL.with(|rc| rc.borrow_mut().pop());
    match maybe {
        Some(buf) => Buffer(Some(buf)),
        None => Buffer(Some(Box::new([0; PAGE_SZ]))),
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let buf = self.0.take().unwrap();
        POOL.with(|rc| rc.borrow_mut().push(buf))
    }
}

impl std::ops::Deref for Buffer {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap().as_ref()
    }
}

impl std::ops::DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap().as_mut()
    }
}
