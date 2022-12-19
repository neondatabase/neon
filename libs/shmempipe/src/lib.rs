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

pub mod shared;

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
    // this wouldn't be too difficult to make a generic parameter, but let's hold off still.
    // pub to_worker: ringbuf::SharedRb<u8, [MaybeUninit<u8>; 128 * 1024]>,
    // pub from_worker: ringbuf::SharedRb<u8, [MaybeUninit<u8>; 8192 * 2]>,
}

// Ideas:
// should this be no_std? couldn't find any issues of using std through c.
// struct ResponseSlot(shared::Mutex<[u8; 8192]>);

pub fn create(path: &Path) -> std::io::Result<SharedMemPipePtr<Ready>> {
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

fn initialize_at(res: SharedMemPipePtr<MMapped>) -> std::io::Result<SharedMemPipePtr<Ready>> {
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

        // Safety: atomics don't need to be init
        let magic = unsafe { magic.assume_init_mut() };

        // we can now be raced by some other process due to shm_open, so write that we are
        // initializing
        magic.store(0, std::sync::atomic::Ordering::SeqCst);
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

    // Safety: it is now initialized
    let _ = unsafe { place.assume_init_mut() };
    drop(place);

    let res = res.post_initialization();

    // FIXME: how exactly to do an Arc out of this? Maybe an Arc<Box<RawSharedMemPipe>>, since we
    // cannot access ArcInner ... which does have a repr(c) but the layout would be version
    // dependent... maybe the custom arc crate with only strong counts?
    //
    // Or just give deref to SharedMemPipePtr and that's it, the ptr can be Arc'd

    res.magic
        .store(0xcafebabe, std::sync::atomic::Ordering::SeqCst);

    res.ref_count
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    Ok(res)
}

/// Type state for the cleanup on drop pointer.
///
/// Without any test specific configuration, will call `munmap` afterwards.
pub struct MMapped;

/// Type state to fully cleanup on drop pointer.
///
/// In addition to `munmap` this will tombstone the shared memory segment, maybe run drop in
/// future.
pub struct Ready;

pub struct SharedMemPipePtr<Stage> {
    ptr: Option<NonNull<RawSharedMemPipe>>,
    size: NonZeroUsize,
    attempt_drop: bool,
    #[cfg(test)]
    munmap: bool,
    _marker: std::marker::PhantomData<Stage>,
}

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

    fn post_initialization(mut self) -> SharedMemPipePtr<Ready> {
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
        use shared::{MutexGuard, PinnedMutex, TryLockError};
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

impl std::ops::Deref for SharedMemPipePtr<Ready> {
    type Target = RawSharedMemPipe;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().unwrap().as_ref() }
    }
}

pub fn open_existing(path: &Path) -> std::io::Result<SharedMemPipePtr<Ready>> {
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

fn join_initialized_at(res: SharedMemPipePtr<MMapped>) -> std::io::Result<SharedMemPipePtr<Ready>> {
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

        // Safety: this should be fine as well, but there might be an issue with *place.as_mut_ptr
        // while there's another pointer to the value?
        let ref_count = unsafe {
            std::ptr::addr_of_mut!((*place.as_mut_ptr()).ref_count)
                .cast::<MaybeUninit<AtomicU32>>()
                .as_mut()
                .expect("valid non-null pointer")
        };

        let ref_count = unsafe { ref_count.assume_init_ref() };

        let count_was = ref_count.fetch_add(1, SeqCst);
        let _g = RefCountDropGuard(ref_count);

        if count_was == 0 {
            // we've resurrected a shared memory area being destroyed, probably don't venture any
            // further
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "shared memory area is being destroyed",
            ));
        }

        let mut ready = false;

        for _ in 0..1000 {
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

        std::mem::forget(_g);
    }

    let res = res.post_initialization();

    Ok(res)
}

struct RefCountDropGuard<'a>(&'a AtomicU32);

impl Drop for RefCountDropGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
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
            assert_eq!(1, ready.ref_count.load(SeqCst));
        }

        // first allowing for initialization then allowing joining already initialized shouldn't
        // cause any more problems, but we might suffer the wait. TODO: make it configurable.

        let joined = {
            let ptr = SharedMemPipePtr::post_mmap_but_no_munmap(ptr.cast(), size);
            super::join_initialized_at(ptr).unwrap()
        };

        {
            assert_eq!(0xcafe_babe, joined.magic.load(SeqCst));
            assert_eq!(2, joined.ref_count.load(SeqCst));
        }

        drop(joined);

        {
            assert_eq!(0xcafe_babe, ready.magic.load(SeqCst));
            assert_eq!(1, ready.ref_count.load(SeqCst));
        }

        drop(ready);

        // the memory is still valid, it hasn't been dropped, the guard will drop it
        {
            let target = ptr.cast::<RawSharedMemPipe>();
            let target = unsafe { target.as_ref() };
            assert_eq!(0xffff_ffff, target.magic.load(SeqCst));
            assert_eq!(0, target.ref_count.load(SeqCst));
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
