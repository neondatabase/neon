#![allow(dead_code)]
use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;

use nix::sys::mman::{MapFlags, ProtFlags};
use shared::TryLockError;

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

    pub participants: [shared::PinnedMutex<Option<u32>>; 2],

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
        let m = unsafe { std::pin::Pin::new_unchecked(&self.participants[0]) };
        let mut guard = m.try_lock().map(Some).unwrap_or_else(|e| match e {
            TryLockError::PreviousOwnerDied(g) => Some(g),
            TryLockError::WouldBlock => None,
        })?;

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
        *guard = Some(std::process::id());
        drop(guard);

        Some(std::sync::Arc::new(OwnedRequester {
            producer: std::sync::Mutex::default(),
            consumer: std::sync::Mutex::default(),
            not_my_time: std::sync::Condvar::new(),
            ptr: self,
        }))
    }
}

impl SharedMemPipePtr<Joined> {
    pub fn try_acquire_responder(self) -> Option<OwnedResponder> {
        let m = unsafe { std::pin::Pin::new_unchecked(&self.participants[1]) };
        let guard = m.try_lock().map(Some).unwrap_or_else(|e| match e {
            TryLockError::PreviousOwnerDied(g) => Some(g),
            TryLockError::WouldBlock => None,
        })?;
        Some(OwnedResponder {
            // Safety: the pointer cannot be moved
            locked_mutex: unsafe { std::mem::transmute(guard) },
            ptr: self,
        })
    }
}

pub struct OwnedRequester {
    producer: std::sync::Mutex<u32>,
    consumer: std::sync::Mutex<u32>,
    not_my_time: std::sync::Condvar,
    ptr: SharedMemPipePtr<Created>,
}

impl OwnedRequester {
    pub fn request_response(&self, req: &[u8], resp: &mut [u8]) {
        let id = {
            let mut g = self.producer.lock().unwrap();

            let id = *g;
            *g = g.wrapping_add(1);

            // Safety: we are only one creating producers for to_worker
            let mut p = unsafe { ringbuf::Producer::new(&self.ptr.to_worker) };
            let m = unsafe { std::pin::Pin::new_unchecked(&self.ptr.to_worker_writer) };
            let c = unsafe { std::pin::Pin::new_unchecked(&self.ptr.to_worker_cond) };

            let mut req = req;

            while !req.is_empty() {
                let n = p.push_slice(req);
                req = &req[n..];

                if n == 0 {
                    // TODO: this mutex does not need to be robust
                    let g = m.lock().expect("cannot have process deaths here");
                    let _ = c.wait_while(g, |_| p.is_full());
                } else {
                    std::thread::yield_now();
                }
            }

            // println!("sent {id}");
            id
        };

        let g = self.consumer.lock().unwrap();
        // wait until it's our turn
        let mut g = self
            .not_my_time
            .wait_while(g, |g| {
                // println!("waiting for {id} turn to read, {} now", *g);
                *g != id
            })
            .unwrap();

        {
            // Safety: we are the only one creating consumers for from_worker
            let mut c = unsafe { ringbuf::Consumer::new(&self.ptr.from_worker) };
            let cond = unsafe { std::pin::Pin::new_unchecked(&self.ptr.from_worker_cond) };

            let mut read = 0;
            loop {
                let n = c.pop_slice(&mut resp[read..]);

                read += n;

                if read == resp.len() {
                    break;
                } else {
                    cond.notify_one();
                    std::thread::yield_now();
                }
            }
        }

        *g = g.wrapping_add(1);
        // there is a crate for better futex usage, which would allow to wake up only the one
        // correct
        self.not_my_time.notify_all();
    }
}

pub struct OwnedResponder {
    // self referential, has to be, also must be above ptr to get dropped first
    locked_mutex: shared::MutexGuard<'static, Option<u32>>,
    ptr: SharedMemPipePtr<Joined>,
}

impl OwnedResponder {
    pub fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::io::Read;
        let mut c = unsafe { ringbuf::Consumer::new(&self.ptr.to_worker) };
        let cond = unsafe { std::pin::Pin::new_unchecked(&self.ptr.to_worker_cond) };

        let mut read = 0;

        loop {
            match c.read(&mut buf[read..]) {
                Ok(n) => {
                    read += n;

                    if read == buf.len() {
                        cond.notify_one();
                        return Ok(n);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    cond.notify_one();
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => {
                    // unexpected, a new impl sends out these?
                    return Err(e);
                }
            }
        }
    }

    pub fn write_all(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let mut p = unsafe { ringbuf::Producer::new(&self.ptr.from_worker) };
        let mutex = unsafe { std::pin::Pin::new_unchecked(&self.ptr.from_worker_writer) };
        let cond = unsafe { std::pin::Pin::new_unchecked(&self.ptr.from_worker_cond) };

        if buf.is_empty() {
            return Ok(0);
        }

        // let mut busy = 0;

        loop {
            let wrote = p.push_slice(buf);
            buf = &buf[wrote..];

            if buf.is_empty() {
                return Ok(0);
            } else {
                // busy += 1;
                let g = mutex.lock().unwrap_or_else(|e| e.into_inner());
                let _ = cond.wait_while(g, |_| p.free_len() == 0);
            }
        }
    }
}

pub fn create(path: &Path) -> std::io::Result<SharedMemPipePtr<Created>> {
    use nix::fcntl::OFlag;
    use nix::sys::mman;
    use nix::sys::stat::Mode;
    use std::os::unix::io::FromRawFd;

    assert!(path.is_absolute());
    assert!(path.as_os_str().len() < 255);

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

    initialize_at(res)
}

/// Initialize the RawSharedMemPipe *in place*.
///
/// In place initialization is trickier than normal rust programs. This would be much simpler if we
/// would have stable allocator trait, and many currently unstable MaybeUninit friendly
/// conversions.
fn initialize_at(res: SharedMemPipePtr<MMapped>) -> std::io::Result<SharedMemPipePtr<Created>> {
    let inner = res.ptr();
    // Safety: lot of requirements, TODO
    let place = unsafe { inner.cast::<MaybeUninit<RawSharedMemPipe>>().as_mut() };

    {
        let magic = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).magic)
                .cast::<MaybeUninit<AtomicU32>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        magic.write(AtomicU32::new(0x0000_0000));

        // ceremonial
        unsafe { magic.assume_init_mut() };
    }

    {
        let participants = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).participants)
                // these casts are easy to get wrong, for example u32 vs. Option<u32> at the
                // deepest level -- maybe this could be done in phases with a helper method to
                // switch only the topmost as maybeuninit
                .cast::<MaybeUninit<[MaybeUninit<shared::PinnedMutex<Option<u32>>>; 2]>>()
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
        let to_worker = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).to_worker)
                .cast::<MaybeUninit<ringbuf::StaticRb<u8, TO_WORKER_LEN>>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        to_worker.write(ringbuf::StaticRb::default());

        unsafe {
            to_worker.assume_init_mut();
        }
    }

    {
        let to_worker_writer = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).to_worker_writer)
                .cast::<MaybeUninit<shared::PinnedMutex<()>>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        shared::PinnedMutex::initialize_at(to_worker_writer, ()).unwrap();
    }

    {
        let to_worker_cond = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).to_worker_cond)
                .cast::<MaybeUninit<shared::PinnedCondvar>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        shared::PinnedCondvar::initialize_at(to_worker_cond).unwrap();
    }

    {
        let from_worker = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).from_worker)
                .cast::<MaybeUninit<ringbuf::StaticRb<u8, FROM_WORKER_LEN>>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        from_worker.write(ringbuf::StaticRb::default());

        unsafe {
            from_worker.assume_init_mut();
        }
    }

    {
        let from_worker_writer = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).from_worker_writer)
                .cast::<MaybeUninit<shared::PinnedMutex<()>>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        shared::PinnedMutex::initialize_at(from_worker_writer, ()).unwrap();
    }

    {
        let from_worker_cond = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).from_worker_cond)
                .cast::<MaybeUninit<shared::PinnedCondvar>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        shared::PinnedCondvar::initialize_at(from_worker_cond).unwrap();
    }

    // FIXME: above, we need to do manual drop handling

    // Safety: it is now initialized
    let _ = unsafe { place.assume_init_mut() };
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
        use std::pin::Pin;

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
                if self.attempt_drop {
                    let shared = unsafe { ptr.as_ref() };

                    let locked = lock_all(&shared.participants);

                    if locked.iter().all(|x| x.is_some()) {
                        // in case anyone still joins, they'll first find this tombstone
                        shared.magic.store(0xffff_ffff, SeqCst);

                        drop(locked);

                        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };

                        // now we are good to drop in place, if need be
                    }
                }

                #[allow(unused)]
                let do_unmap = true;
                #[cfg(test)]
                let do_unmap = self.munmap;

                if do_unmap {
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

pub fn open_existing(path: &Path) -> std::io::Result<SharedMemPipePtr<Joined>> {
    use nix::fcntl::OFlag;
    use nix::sys::mman;
    use nix::sys::stat::Mode;
    use std::os::unix::io::FromRawFd;

    assert!(path.is_absolute());
    assert!(path.as_os_str().len() < 255);

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
