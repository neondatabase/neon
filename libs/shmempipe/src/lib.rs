#![allow(dead_code)]
use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::FromRawFd;
use std::path::Path;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};

use nix::sys::mman::{MapFlags, ProtFlags};
use shared::{IntoGuard, TryLockError};

pub mod shared;

const TO_WORKER_LEN: usize = 32 * 4096;
const FROM_WORKER_LEN: usize = 4 * 4096;

/// Input/output over a shared memory "pipe" which attempts to be faster than using standard input
/// and output with inter-process communication.
///
/// repr(C): this struct could be shared between recompilations.
// TODO: this should be inside manuallydrop?
#[repr(C)]
pub struct RawSharedMemPipe {
    /// States:
    /// - 0x0000_0000 means initializing
    /// - 0xcafe_babe means ready
    /// - 0xffff_ffff means tearing down
    pub magic: AtomicU32,

    /// Eventfd used in semaphore mode, used
    pub notify_request_written: i32,

    pub notify_response_written: i32,

    /// The processes participating in this.
    ///
    /// First is the pageserver process, second is the single threaded walredo process.
    ///
    /// FIXME: these are unsafe in security barriers.
    pub participants: [shared::PinnedMutex<Option<u32>>; 2],

    pub to_worker_waiters: AtomicU32,
    pub to_worker_writer: shared::PinnedMutex<()>,
    pub to_worker_cond: shared::PinnedCondvar,

    pub from_worker_writer: shared::PinnedMutex<()>,
    pub from_worker_cond: shared::PinnedCondvar,

    // this wouldn't be too difficult to make a generic parameter, but let's hold off still.
    //
    // TODO: heikki wanted the response channel to be N * 8192 bytes, aligned to page so that they
    // could possibly in future be mapped postgres shared buffers.
    //
    // Note: this is repr(c), so the order matters.
    pub to_worker: ringbuf::SharedRb<u8, [MaybeUninit<u8>; TO_WORKER_LEN]>,
    pub from_worker: ringbuf::SharedRb<u8, [MaybeUninit<u8>; FROM_WORKER_LEN]>,
}

impl SharedMemPipePtr<Created> {
    /// Wrap this in a new hopefully unique `Arc<OwnedRequester>`.
    pub fn try_acquire_requester(self) -> Option<std::sync::Arc<OwnedRequester>> {
        let m = unsafe { Pin::new_unchecked(&self.participants[0]) };
        let mut guard = m.try_lock().into_guard()?;

        match *guard {
            Some(x) if x == std::process::id() => {
                // hopefully a re-acquiring
            }
            Some(_other) => return None,
            None => {}
        }

        // well, we cannot really do much more than this. I was initially planning to keep the
        // mutex locked, but that would have zero guarantees that the thread which created this
        // side is the one to drop it.
        //
        // could hold a semaphore instead?
        *guard = Some(std::process::id());
        drop(guard);

        Some(std::sync::Arc::new(OwnedRequester {
            producer: std::sync::Mutex::default(),
            consumer: std::sync::Mutex::default(),
            ptr: self,
        }))
    }
}

impl SharedMemPipePtr<Joined> {
    pub fn try_acquire_responder(self) -> Option<OwnedResponder> {
        let m = unsafe { Pin::new_unchecked(&self.participants[1]) };
        let guard = m.try_lock().into_guard()?;
        Some(OwnedResponder {
            // Safety: the pointer `ptr` will not be remapped
            locked_mutex: unsafe { std::mem::transmute(guard) },
            ptr: self,
            remaining: None,
        })
    }
}

pub struct OwnedRequester {
    producer: std::sync::Mutex<u32>,
    consumer: std::sync::Mutex<Wakeup>,
    ptr: SharedMemPipePtr<Created>,
}

#[derive(Default)]
struct Wakeup {
    waiting: UnparkInOrder,
    next: u32,
}

#[derive(Default)]
struct UnparkInOrder(std::collections::VecDeque<Option<std::thread::Thread>>);

impl UnparkInOrder {
    fn store_current(&mut self, distance: usize) {
        // it was thought originally that this would be *enough*, as in we'd unlikely have so many
        // threads waiting that we'd have to have an alternative place for the overflow to go.
        //
        // let's check the popular thread count invariant :)
        assert!(distance < 4096);
        while self.0.len() <= distance {
            self.0.push_back(None);
        }
        let slot = self.0.get_mut(distance).expect("just added the None in");
        assert!(slot.is_none());
        *slot = Some(std::thread::current());
    }

    fn current_is_front(&self) -> bool {
        match self.0.front() {
            Some(Some(first)) => {
                let cur = std::thread::current();
                cur.id() == first.id()
            }
            Some(None) | None => false,
        }
    }

    fn pop_current(&mut self) {
        let cur = std::thread::current();
        let next = self.0.front();
        let next = next
            .expect("should not be empty because we were just unparked")
            .as_ref()
            .expect("should had had the current thread in front because we were just unparked");
        assert_eq!(cur.id(), next.id());

        self.0.pop_front().expect("just verified");
    }

    fn unpark_front(&self) {
        if let Some(x) = self.0.front().and_then(|x| x.as_ref()) {
            x.unpark();
        } else {
            // Not an error, the thread we are hoping to wakeup just hasn't yet arrived to the
            // parking lot.
        }
    }

    /// Park the thread waiting to be unparked in order.
    ///
    /// Should create a profiling point.
    #[no_mangle]
    #[inline(never)]
    fn park() {
        std::thread::park();
    }
}

impl OwnedRequester {
    /// Returns the file descriptors that need to be kept open for child process.
    pub fn shared_fds(&self) -> [i32; 2] {
        [
            // FIXME: one should be enough for waiting for the worker, or the worker waiting for
            // new input
            self.ptr.notify_request_written,
            self.ptr.notify_response_written,
        ]
    }

    pub fn request_response(&self, req: &[u8], resp: &mut [u8]) {
        // Overview:
        // - `self.producer` creates an order amongst competing request_response callers (id).
        // - the same token (id) is used to find some order with `self.consumer` to read the
        // response

        let id = self.send_request(req);

        let mut g = self.consumer.lock().unwrap();
        let distance = id.wrapping_sub(g.next) as usize;
        // FIXME: current impl stores the thread even in `id == g.next`
        g.waiting.store_current(distance);

        // TODO: UnparkInOrder::park_while(g, &self.consumer, |g| *g.next == id)
        let mut g = Some(g);
        while g.as_ref().unwrap().next != id {
            drop(g.take());
            UnparkInOrder::park();
            g = Some(self.consumer.lock().unwrap());
        }

        let g = g.unwrap();
        assert!(g.waiting.current_is_front());

        let mut g = self.recv_response(id, g, resp);

        g.next = g.next.wrapping_add(1);
        g.waiting.pop_current();
        g.waiting.unpark_front();
    }

    fn send_request(&self, req: &[u8]) -> u32 {
        let mut g = self.producer.lock().unwrap();

        let mut might_wait = self.ptr.to_worker_waiters.fetch_add(1, Release) == 0;

        let id = *g;
        *g = g.wrapping_add(1);

        // Safety: we are only one creating producers for to_worker
        let mut p = unsafe { ringbuf::Producer::new(&self.ptr.to_worker) };

        let sem = unsafe { shared::EventfdSemaphore::from_raw_fd(self.ptr.notify_request_written) };

        let mut send = |mut req| {
            let mut consecutive_spins = 0;
            loop {
                let n = p.push_slice(req);
                req = &req[n..];

                if req.is_empty() {
                    break;
                } else if n == 0 && might_wait {
                    sem.post();
                    might_wait = false;
                } else if n != 0 {
                    consecutive_spins = 0;
                    std::hint::spin_loop();
                } else if consecutive_spins < 1024 {
                    consecutive_spins += 1;
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
            }
        };

        // framing doesn't require us to manage state, as we always send both
        // TODO: try with postponed producer, this syncs twice, might might be a good or bad thing.
        let frame_len = u32::try_from(req.len())
            .expect("message cannot be more than 4GB")
            .to_ne_bytes();

        send(&frame_len);
        send(req);

        drop(g);

        // as part of the first write, make sure that the worker is woken up.
        if might_wait {
            sem.post();
        }

        id
    }

    fn recv_response<'a>(
        &self,
        id: u32,
        g: std::sync::MutexGuard<'a, Wakeup>,
        resp: &mut [u8],
    ) -> std::sync::MutexGuard<'a, Wakeup> {
        assert_eq!(g.next, id);

        // Safety: we are the only one creating consumers for from_worker
        let mut c = unsafe { ringbuf::Consumer::new(&self.ptr.from_worker) };

        let sem =
            unsafe { shared::EventfdSemaphore::from_raw_fd(self.ptr.notify_response_written) };

        sem.wait();

        let mut read = 0;
        let mut consecutive_spins = 0;

        loop {
            let n = c.pop_slice(&mut resp[read..]);

            read += n;

            if read == resp.len() {
                break;
            }

            if n != 0 {
                consecutive_spins = 0;
                std::hint::spin_loop();
            } else if consecutive_spins < 1024 {
                consecutive_spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }

        // now the wait on the condition can start on `postgres --wal-redo` side
        self.ptr.to_worker_waiters.fetch_sub(1, Release);

        g
    }
}

/// This type is movable.
#[repr(C)]
pub struct OwnedResponder {
    // self referential, has to be, also must be above ptr to get dropped first
    locked_mutex: shared::MutexGuard<'static, Option<u32>>,
    /// How long currently received message is, and how much is remaining.
    remaining: Option<(u32, u32)>,
    ptr: SharedMemPipePtr<Joined>,
}

impl OwnedResponder {
    pub fn read_next_frame_len(&mut self) -> Result<u32, u32> {
        match self.remaining.as_mut() {
            Some((_, remaining)) => Err(*remaining),
            None => {
                // well, reading to empty does seem wrong
                assert_eq!(self.read(&mut [][..]), 0);
                let (len, remaining) = self.remaining.as_ref().unwrap();
                assert_eq!(len, remaining);
                return Ok(*remaining);
            }
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        if self.remaining.is_none() {
            // read the new frame length
            let mut len = [0u8; 4];
            assert_eq!(self.recv(&mut len, 3), 4);
            let len = u32::from_ne_bytes(len);
            // store it as frame size, remaining size
            self.remaining = Some((len, len));
        }

        if buf.is_empty() {
            return 0;
        }

        let (_, mut remaining) = self.remaining.unwrap();

        // recv only up to the next frame length
        let allowed = buf.len();
        let buf = &mut buf[..std::cmp::min(allowed, remaining as usize)];

        let read = self.recv(buf, 0);

        remaining = remaining
            .checked_sub(
                u32::try_from(read)
                    .expect("should had read at most remaining, not overflowing u32"),
            )
            .expect("should not have read more than remaining");

        if remaining == 0 {
            self.remaining = None;
        } else {
            let (_, rem) = self.remaining.as_mut().unwrap();
            *rem = remaining;
        }

        read
    }

    // TODO: call this read_frame or something other
    pub fn read_exact(&mut self, buf: &mut [u8]) -> usize {
        // TODO: panics should not be leaked to ffi, it is UB right now but might become abort in
        // future. it is easy to take all common pointer handling out and make that wrapper also
        // catch_unwind, then abort.
        // eprintln!("read_exact({})", buf.len());
        let remaining = match self.remaining.as_ref() {
            Some((_, remaining)) => *remaining,
            None => unreachable!("cannot panic here but the frame length should be known"),
        };

        assert!(remaining as usize <= buf.len());

        // can remaining be 1? probably by bug.
        assert!(remaining > 1);

        let read = self.recv(&mut buf[..remaining as usize], remaining as usize - 1);

        assert_eq!(read, remaining as usize);
        self.remaining = None;

        // eprintln!("read_exact({}) -> {}", buf.len(), read);
        read
    }

    fn recv(&mut self, buf: &mut [u8], read_more_than: usize) -> usize {
        let mut c = unsafe { ringbuf::Consumer::new(&self.ptr.to_worker) };
        let sem = unsafe { shared::EventfdSemaphore::from_raw_fd(self.ptr.notify_request_written) };

        let mut read = 0;
        let mut waited = false;
        loop {
            let n = c.pop_slice(&mut buf[read..]);

            read += n;

            if read > read_more_than {
                return read;
            } else if n == 0 && !waited {
                while self.ptr.to_worker_waiters.load(Acquire) == 0 {
                    sem.wait();
                    waited = true;
                }

                if waited {
                    continue;
                }

                std::hint::spin_loop();
            } else {
                // std::thread::yield_now();
                std::hint::spin_loop();
            }
        }
    }

    pub fn write_all(&mut self, mut buf: &[u8]) -> usize {
        let mut p = unsafe { ringbuf::Producer::new(&self.ptr.from_worker) };
        // let _m = unsafe { Pin::new_unchecked(&self.ptr.from_worker_writer) };
        // let _cond = unsafe { Pin::new_unchecked(&self.ptr.from_worker_cond) };

        let sem =
            unsafe { shared::EventfdSemaphore::from_raw_fd(self.ptr.notify_response_written) };

        if buf.is_empty() {
            return 0;
        }

        sem.post();

        let len = buf.len();

        let mut woken = false;
        let mut consecutive_spins = 0;

        loop {
            let n = p.push_slice(buf);
            buf = &buf[n..];

            if !woken {
                woken = true;
                sem.post();
            }

            if buf.is_empty() {
                return len;
            }

            if n != 0 {
                consecutive_spins = 0;
                std::hint::spin_loop();
            } else if consecutive_spins < 1024 {
                consecutive_spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

// TODO: cbindgen could probably just output the header file for these functions

/// Main entrypoint for the pgxn/neon_walredo/walredoproc.c.
///
/// Reads the "WALREDO_TENANT" environment variable which is expected to have the hex form of
/// tenant id in it, uses that as the suffix of the shm_open path.
#[cfg(target_os = "linux")]
#[no_mangle]
pub extern "C" fn shmempipe_open_via_env() -> *mut OwnedResponder {
    use std::os::unix::ffi::OsStrExt;

    let id = match std::env::var_os("WALREDO_TENANT") {
        Some(x) if x.len() == 32 => x,
        Some(_) | None => return std::ptr::null_mut(),
    };

    let mut buf = [0u8; 9 + 32 + 1];
    b"/walredo-"
        .into_iter()
        .copied()
        .chain(id.as_bytes().into_iter().copied())
        .chain(std::iter::once(0))
        .zip(buf.iter_mut())
        .for_each(|(i, o)| *o = i);

    let path = match std::ffi::CStr::from_bytes_with_nul(&buf) {
        Ok(path) => path,
        Err(_) => return std::ptr::null_mut(),
    };

    match open_existing(path).map(|joined| joined.try_acquire_responder()) {
        Ok(Some(responder)) => Box::into_raw(Box::new(responder)),
        Ok(None) | Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn shmempipe_read_frame_len(
    resp: *mut OwnedResponder,
    len: *mut u32,
) -> libc::c_int {
    if resp.is_null() || len.is_null() {
        return -1;
    }

    let mut target = unsafe { Box::from_raw(resp) };
    let res = target.read_next_frame_len();
    std::mem::forget(target);
    match res {
        Ok(frame_len) => {
            unsafe { len.write(frame_len) };
            0
        }
        Err(_) => return -2,
    }
}

#[no_mangle]
pub extern "C" fn shmempipe_read(resp: *mut OwnedResponder, buffer: *mut u8, len: u32) -> isize {
    if resp.is_null() || buffer.is_null() {
        return -1;
    }
    if len == 0 {
        return 0;
    }
    let mut target = unsafe { Box::from_raw(resp) };
    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, len as usize) };
    let ret = target.read(buffer);
    std::mem::forget(target);
    ret as isize
}

#[no_mangle]
pub extern "C" fn shmempipe_read_exact(
    resp: *mut OwnedResponder,
    buffer: *mut u8,
    len: u32,
) -> isize {
    if resp.is_null() || buffer.is_null() {
        return -1;
    }
    if len == 0 {
        return 0;
    }
    let mut target = unsafe { Box::from_raw(resp) };
    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, len as usize) };
    target.read_exact(buffer);
    std::mem::forget(target);
    len as isize
}

#[no_mangle]
pub extern "C" fn shmempipe_write_all(
    resp: *mut OwnedResponder,
    buffer: *mut u8,
    len: u32,
) -> isize {
    if resp.is_null() || buffer.is_null() {
        return -1;
    }
    if len == 0 {
        return 0;
    }
    let mut target = unsafe { Box::from_raw(resp) };
    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, len as usize) };
    let ret = target.write_all(buffer);
    std::mem::forget(target);
    ret as isize
}

#[no_mangle]
pub extern "C" fn shmempipe_destroy(resp: *mut OwnedResponder) {
    if !resp.is_null() {
        unsafe { Box::from_raw(resp) };
    }
}

pub fn create(path: &Path) -> std::io::Result<SharedMemPipePtr<Created>> {
    use nix::fcntl::OFlag;
    use nix::sys::eventfd::{eventfd, EfdFlags};
    use nix::sys::mman;
    use nix::sys::stat::Mode;

    assert!(path.is_absolute());
    assert!(path.as_os_str().len() < 255);

    // synchronization between the creator and the
    // FIXME: OwnedFd
    let notify_request_written =
        unsafe { std::fs::File::from_raw_fd(eventfd(0, EfdFlags::EFD_SEMAPHORE)?) };
    let notify_response_written =
        unsafe { std::fs::File::from_raw_fd(eventfd(0, EfdFlags::EFD_SEMAPHORE)?) };

    // O_CLOEXEC, maybe?
    let flags = OFlag::O_CREAT | OFlag::O_RDWR | OFlag::O_TRUNC | OFlag::O_CLOEXEC;
    let mode = Mode::S_IRUSR | Mode::S_IWUSR;

    // use it as a file for get automatic closing
    // FIXME: should use OwnedFd but unstable
    let handle = unsafe { std::fs::File::from_raw_fd(mman::shm_open(path, flags, mode)?) };

    let size = Layout::new::<RawSharedMemPipe>()
        .align_to(4096)
        .expect("alignment is power of two")
        .size();

    assert!(size > 0);

    handle.set_len(size as u64)?;

    let size = NonZeroUsize::new(size).unwrap();

    let ptr = unsafe {
        // Safety: ffi(?)
        mman::mmap(
            None,
            size,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
            MapFlags::MAP_SHARED,
            handle.as_raw_fd(),
            0,
        )
    }?;

    let ptr = NonNull::new(ptr).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "mmap returned null pointer")
    })?;

    // use this on stack for panics until init is complete, then Arc it?
    let res = SharedMemPipePtr::post_mmap(ptr.cast::<RawSharedMemPipe>(), size);

    // file is no longer needed -- or is it? should it be saved and cleared? we might be leaking
    // fd's, unless the mmap's hold an "fd" to the shared
    drop(handle);

    initialize_at(res, notify_request_written, notify_response_written)
}

/// Initialize the RawSharedMemPipe *in place*.
///
/// In place initialization is trickier than normal rust programs. This would be much simpler if we
/// would have stable allocator trait, and many currently unstable MaybeUninit friendly
/// conversions.
fn initialize_at(
    res: SharedMemPipePtr<MMapped>,
    notify_request_written: std::fs::File,
    notify_response_written: std::fs::File,
) -> std::io::Result<SharedMemPipePtr<Created>> {
    let inner = res.ptr();
    // Safety: lot of requirements, TODO
    let place = unsafe { inner.cast::<MaybeUninit<RawSharedMemPipe>>().as_mut() };

    trait AsPointerToUninit {
        type Target;
        fn cast_uninit(self) -> Self::Target;
    }

    trait AsPointerToUninitArray {
        type Target;
        fn cast_uninit_array(self) -> Self::Target;
    }

    impl<T> AsPointerToUninit for *mut T {
        type Target = *mut MaybeUninit<T>;

        // this is just a convinience to type less, also, any cast is valid, so this is easy to
        // mistype
        fn cast_uninit(self) -> Self::Target {
            self.cast::<MaybeUninit<T>>()
        }
    }

    impl<T, const N: usize> AsPointerToUninitArray for *mut [T; N] {
        type Target = *mut [MaybeUninit<T>; N];

        fn cast_uninit_array(self) -> Self::Target {
            self.cast::<[MaybeUninit<T>; N]>()
        }
    }

    macro_rules! uninit_field {
        ($field:ident) => {{
            unsafe {
                std::ptr::addr_of_mut!((*place.as_mut_ptr()).$field)
                    .cast_uninit()
                    .as_mut()
                    .expect("valid non-null ptr")
            }
        }};
    }

    {
        let magic = uninit_field!(magic);
        magic.write(AtomicU32::new(0x0000_0000));

        // ceremonial
        unsafe { magic.assume_init_mut() };
    }

    {
        let fd = uninit_field!(notify_request_written);
        fd.write(notify_request_written.as_raw_fd());
        unsafe { fd.assume_init_mut() };

        // the file is forgotten if the init completes
    }

    {
        let fd = uninit_field!(notify_response_written);
        fd.write(notify_response_written.as_raw_fd());
        unsafe { fd.assume_init_mut() };

        // the file is forgotten if the init completes
    }

    {
        let participants = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).participants)
                .cast_uninit_array()
                .cast_uninit()
                .as_mut()
                .expect("valid non-null pointer")
        };

        // Safety: array_assume_init is unstable
        let participants = unsafe { participants.assume_init_mut() };

        let mut initialized = 0;

        for slot in participants.iter_mut() {
            // panic safety: is not
            match shared::PinnedMutex::initialize_at(slot, None) {
                Ok(_) => initialized += 1,
                Err(e) => {
                    participants[..initialized]
                        .iter_mut()
                        // Safety: initialized up to `initialized`
                        .for_each(|x| unsafe { x.assume_init_drop() });

                    return Err(e);
                }
            }
        }
    }

    {
        let to_worker_waiters = uninit_field!(to_worker_waiters);
        to_worker_waiters.write(AtomicU32::default());
        unsafe { to_worker_waiters.assume_init_mut() };
    }

    {
        let to_worker = uninit_field!(to_worker);
        to_worker.write(ringbuf::StaticRb::default());
        unsafe { to_worker.assume_init_mut() };
    }

    {
        let to_worker_writer = uninit_field!(to_worker_writer);
        shared::PinnedMutex::initialize_at(to_worker_writer, ()).unwrap();
    }

    {
        let to_worker_cond = uninit_field!(to_worker_cond);
        shared::PinnedCondvar::initialize_at(to_worker_cond).unwrap();
    }

    {
        let from_worker = uninit_field!(from_worker);
        from_worker.write(ringbuf::StaticRb::default());
        unsafe { from_worker.assume_init_mut() };
    }

    {
        let from_worker_writer = uninit_field!(from_worker_writer);
        shared::PinnedMutex::initialize_at(from_worker_writer, ()).unwrap();
    }

    {
        let from_worker_cond = uninit_field!(from_worker_cond);
        shared::PinnedCondvar::initialize_at(from_worker_cond).unwrap();
    }

    // FIXME: above, we need to do manual drop handling

    // Safety: it is now initialized
    let _ = unsafe { place.assume_init_mut() };
    std::mem::forget(notify_request_written);
    std::mem::forget(notify_response_written);
    drop(place);

    let res = res.post_initialization::<Created>();

    // FIXME: how exactly to do an Arc out of this? Maybe an Arc<Box<RawSharedMemPipe>>, since we
    // cannot access ArcInner ... which does have a repr(c) but the layout would be version
    // dependent... maybe the custom arc crate with only strong counts?
    //
    // Or just give deref to SharedMemPipePtr and that's it, the ptr can be Arc'd

    res.magic
        .store(0xcafebabe, std::sync::atomic::Ordering::SeqCst);

    // FIXME: it is very ackward to *not* take the lock participants[0] here. We could have an
    // additional wrapper data structure living in where-ever, which would record that a lock was
    // taken and it needs to be unlocked before drop or better yet, have that happen automatically.

    Ok(res)
}

/// Type state for the cleanup on drop pointer.
///
/// Without any test specific configuration, will call `munmap` afterwards.
pub struct MMapped;

/// Type state to fully cleanup on drop pointer, created with [`create`].
pub struct Created;

/// Type state to fully cleanup on drop pointer, created with [`open_existing`].
pub struct Joined;

pub struct SharedMemPipePtr<Stage> {
    ptr: Option<NonNull<RawSharedMemPipe>>,
    size: NonZeroUsize,
    attempt_drop: bool,
    #[cfg(test)]
    munmap: bool,
    _marker: std::marker::PhantomData<Stage>,
}

unsafe impl Send for SharedMemPipePtr<Created> {}
unsafe impl Sync for SharedMemPipePtr<Created> {}

impl SharedMemPipePtr<MMapped> {
    fn post_mmap(ptr: NonNull<RawSharedMemPipe>, size: NonZeroUsize) -> Self {
        SharedMemPipePtr {
            ptr: Some(ptr),
            size,
            attempt_drop: false,
            #[cfg(test)]
            munmap: true,
            _marker: std::marker::PhantomData,
        }
    }

    #[cfg(test)]
    fn post_mmap_but_no_munmap(ptr: NonNull<RawSharedMemPipe>, size: NonZeroUsize) -> Self {
        SharedMemPipePtr {
            ptr: Some(ptr),
            size,
            attempt_drop: false,
            munmap: false,
            _marker: std::marker::PhantomData,
        }
    }

    fn ptr(&self) -> NonNull<RawSharedMemPipe> {
        self.ptr.as_ref().unwrap().clone()
    }

    fn post_initialization<T>(mut self) -> SharedMemPipePtr<T> {
        let ptr = self.ptr.take();
        let size = self.size;
        let ret = SharedMemPipePtr {
            ptr,
            size,
            attempt_drop: true,
            #[cfg(test)]
            munmap: self.munmap,
            _marker: std::marker::PhantomData,
        };
        std::mem::forget(self);
        ret
    }
}

impl<Stage> Drop for SharedMemPipePtr<Stage> {
    fn drop(&mut self) {
        use shared::{MutexGuard, PinnedMutex};

        // Helper for locking all of the participants.
        fn lock_all<const N: usize>(
            particpants: &[PinnedMutex<Option<u32>>; N],
        ) -> [Option<MutexGuard<'_, Option<u32>>>; N] {
            const NONE: Option<MutexGuard<'_, Option<u32>>> = None;

            let mut res = [NONE; N];

            for (i, m) in particpants.into_iter().enumerate() {
                let m = unsafe { Pin::new_unchecked(m) };
                res[i] = match m.try_lock() {
                    Ok(g) | Err(TryLockError::PreviousOwnerDied(g)) => Some(g),
                    Err(TryLockError::WouldBlock) => None,
                }
            }

            res
        }

        let _res = {
            if let Some(ptr) = self.ptr.take() {
                // use another eventfd for this, something the creator takes during...?
                if false && self.attempt_drop {
                    let shared = unsafe { ptr.as_ref() };

                    // TODO: remove all this
                    let locked = lock_all(&shared.participants);

                    if locked.iter().all(|x| x.is_some()) {
                        // in case anyone still joins, they'll first find this tombstone
                        shared.magic.store(0xffff_ffff, SeqCst);

                        drop(locked);

                        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };

                        // now we are good to drop in place, if need be
                    }
                }

                // FIXME: drop the eventfd somehow, is it dup'd or what?

                #[allow(unused)]
                let do_unmap = true;
                #[cfg(test)]
                let do_unmap = self.munmap;

                if false && do_unmap {
                    // if any locks were still held by other processes, this should not be done
                    // (link kernel robust futex doc here)
                    unsafe { nix::sys::mman::munmap(ptr.as_ptr().cast(), self.size.get()) }
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            }
        };
        #[cfg(debug_assertions)]
        _res.expect("closing SharedMemPipePtr failed");
    }
}

impl std::ops::Deref for SharedMemPipePtr<Created> {
    type Target = RawSharedMemPipe;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().unwrap().as_ref() }
    }
}

impl std::ops::Deref for SharedMemPipePtr<Joined> {
    type Target = RawSharedMemPipe;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().unwrap().as_ref() }
    }
}

pub fn open_existing<P: nix::NixPath + ?Sized>(
    path: &P,
) -> std::io::Result<SharedMemPipePtr<Joined>> {
    use nix::fcntl::OFlag;
    use nix::sys::mman;
    use nix::sys::stat::Mode;

    // assert!(path.is_absolute());
    // assert!(path.as_os_str().len() < 255);

    let flags = OFlag::O_RDWR;
    let mode = Mode::S_IRUSR | Mode::S_IWUSR;

    // use it as a file for get automatic closing
    // FIXME: should use OwnedFd but unstable
    // Safety: ffi?
    let handle = unsafe { std::fs::File::from_raw_fd(mman::shm_open(path, flags, mode)?) };

    let size = Layout::new::<RawSharedMemPipe>()
        .align_to(4096)
        .expect("alignment is power of two")
        .size();

    assert!(size > 0);

    handle.set_len(size as u64)?;

    let size = NonZeroUsize::new(size).unwrap();

    let ptr = unsafe {
        // Safety: ffi(?)
        mman::mmap(
            None,
            size,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
            MapFlags::MAP_SHARED,
            handle.as_raw_fd(),
            0,
        )
    }?;

    let ptr = NonNull::new(ptr).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "mmap returned null pointer")
    })?;

    let ptr = ptr.cast::<RawSharedMemPipe>();

    // use this on stack for panics until init is complete, then Arc it?
    let res = SharedMemPipePtr::post_mmap(ptr, size);

    join_initialized_at(res)
}

fn join_initialized_at(
    res: SharedMemPipePtr<MMapped>,
) -> std::io::Result<SharedMemPipePtr<Joined>> {
    let inner = res.ptr();
    let place = unsafe { inner.cast::<MaybeUninit<RawSharedMemPipe>>().as_mut() };

    // NOTE: here cannot be any mutex initialization
    {
        let magic = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).magic)
                .cast::<MaybeUninit<AtomicU32>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        // Safety: atomics don't need to be init
        let magic = unsafe { magic.assume_init_ref() };

        let mut ready = false;

        for _ in 0..1000 {
            // FIXME: acqrel would be better?
            let read = magic.load(SeqCst);

            match read {
                0x0000_0000 => {
                    // we are early, it's being initialized
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                0xcafe_babe => {
                    // it's ready!
                    ready = true;
                    break;
                }
                other => {
                    // it probably is not healthy
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("shared memory area has unknown magic: 0x{other:08x}"),
                    ));
                }
            }
        }

        if !ready {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("shared memory area did not complete initialization before timeout"),
            ));
        }
    }

    // It is now initialized, but it happened on a different process
    unsafe { place.assume_init_mut() };

    Ok(res.post_initialization())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::SeqCst;
    use std::{mem::MaybeUninit, num::NonZeroUsize, ptr::NonNull};

    use crate::SharedMemPipePtr;

    use super::RawSharedMemPipe;

    /// This is a test for miri to detect any UB, or valgrind memcheck.
    ///
    /// With miri, `parking_lot` simpler mutexes are used.
    #[cfg(miri)]
    #[test]
    fn initialize_at_on_boxed() {
        let mem = Box::new(MaybeUninit::<RawSharedMemPipe>::uninit());
        let ptr = Box::into_raw(mem);

        let _guard = DropRawBoxOnDrop(ptr);

        let ptr = NonNull::new(ptr).unwrap();
        let size = std::mem::size_of::<RawSharedMemPipe>();
        let size = NonZeroUsize::new(size).unwrap();

        // TODO: maybe add Stage::Target = { MaybeUninit<_>, _ }? it is what the types basically
        // do.
        let ready = {
            let ptr = SharedMemPipePtr::post_mmap_but_no_munmap(ptr.cast(), size);
            super::initialize_at(ptr).unwrap()
        };

        {
            assert_eq!(0xcafebabe, ready.magic.load(SeqCst));
        }

        // first allowing for initialization then allowing joining already initialized shouldn't
        // cause any more problems, but we might suffer the wait. TODO: make it configurable.

        let joined = {
            let ptr = SharedMemPipePtr::post_mmap_but_no_munmap(ptr.cast(), size);
            super::join_initialized_at(ptr).unwrap()
        };

        {
            assert_eq!(0xcafe_babe, joined.magic.load(SeqCst));
        }

        drop(joined);

        {
            assert_eq!(0xcafe_babe, ready.magic.load(SeqCst));
        }

        drop(ready);

        // the memory is still valid, it hasn't been dropped, the guard will drop it
        {
            let target = ptr.cast::<RawSharedMemPipe>();
            let target = unsafe { target.as_ref() };
            assert_eq!(0xffff_ffff, target.magic.load(SeqCst));
        }
    }

    struct DropRawBoxOnDrop<T>(*mut T);

    impl<T> Drop for DropRawBoxOnDrop<T> {
        fn drop(&mut self) {
            // Safety: we never deallocate (might munmap) in tests
            unsafe { Box::from_raw(self.0) };
        }
    }
}
