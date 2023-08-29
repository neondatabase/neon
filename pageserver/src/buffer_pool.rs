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

pub(crate) struct PageWriteGuardBuf {
    page: Buffer,
    init_up_to: usize,
}
impl PageWriteGuardBuf {
    pub fn new(buf: Buffer) -> Self {
        PageWriteGuardBuf {
            page: buf,
            init_up_to: 0,
        }
    }
    pub fn assume_init(self) -> Buffer {
        assert_eq!(self.init_up_to, PAGE_SZ);
        self.page
    }
}

// Safety: the [`PageWriteGuard`] gives us exclusive ownership of the page cache slot,
// and the location remains stable even if [`Self`] or the [`PageWriteGuard`] is moved.
unsafe impl tokio_epoll_uring::IoBuf for PageWriteGuardBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.page.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.init_up_to
    }
    fn bytes_total(&self) -> usize {
        self.page.len()
    }
}
// Safety: see above, plus: the ownership of [`PageWriteGuard`] means exclusive access,
// hence it's safe to hand out the `stable_mut_ptr()`.
unsafe impl tokio_epoll_uring::IoBufMut for PageWriteGuardBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.page.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        assert!(pos <= self.page.len());
        self.init_up_to = pos;
    }
}
