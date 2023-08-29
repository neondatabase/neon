use crate::tenant::disk_btree::PAGE_SZ;



pub(crate) type Buffer = Box<[u8; PAGE_SZ]>;

pub(crate) fn get() -> Buffer {
    todo!()
}

pub(crate) fn put(buf: Buffer) {
    todo!()
}
