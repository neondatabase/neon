use std::cell::RefCell;

use crate::tenant::disk_btree::PAGE_SZ;

pub(crate) type Buffer = Box<[u8; PAGE_SZ]>;

// Thread-local list of re-usable buffers.
thread_local! {
    static POOL: RefCell<Vec<Buffer>> = RefCell::new(Vec::new());
}

pub(crate) fn get() -> Buffer {
    let maybe = POOL.with(|rc| rc.borrow_mut().pop());
    match maybe {
        Some(buf) => buf,
        Nonne => Box::new([0; PAGE_SZ]),
    }
}

pub(crate) fn put(buf: Buffer) {
    POOL.with(|rc| rc.borrow_mut().push(buf))
}
