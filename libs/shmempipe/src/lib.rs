#![allow(dead_code)]
use std::{
    alloc::Layout, mem::MaybeUninit, num::NonZeroUsize, os::unix::io::AsRawFd, path::Path,
    pin::Pin, ptr::NonNull, sync::atomic::AtomicU32,
};

use nix::sys::mman::{MapFlags, ProtFlags};

pub mod shared;

/// Input/output over a shared memory "pipe" which attempts to be faster than using standard input
/// and output with inter-process communication.
///
/// repr(C): this struct could be shared between recompilations.
// TODO: this should be inside manuallydrop?
#[repr(C)]
pub struct RawSharedMemPipe {
    pub magic: AtomicU32,
    pub participants: [shared::PinnedMutex<Option<u32>>; 2],
    // to_worker: ringbuf::SharedRb<u8, [MaybeUninit<u8>; 128 * 1024]>,
    // from_worker: ringbuf::SharedRb<u8, [MaybeUninit<u8>; 8192 * 2]>,
}

// Ideas:
// should this be no_std? couldn't find any issues of using std through c.
// struct ResponseSlot(shared::Mutex<[u8; 8192]>);

pub fn create(path: &Path) -> std::io::Result<SharedMemPipePtr> {
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
    let mut res = SharedMemPipePtr(Some(ptr.cast::<RawSharedMemPipe>()), size, false);

    // file is no longer needed -- or is it? should it be saved and cleared? we might be leaking
    // fd's, unless the mmap's hold an "fd" to the shared
    drop(handle);

    let inner = res.ptr().unwrap();
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

        let mut slots = participants.iter_mut();

        let first = slots.next().expect("it has the first");

        let our = shared::PinnedMutex::initialize_at(first, Some(std::process::id()))?;
        let our = our.as_ref();

        let our_guard = our
            .try_lock()
            .expect("should had been able to lock our process' mutex right away after init");

        let mut initialized = 0;

        for slot in slots {
            // panic safety: is not
            match shared::PinnedMutex::initialize_at(slot, None) {
                Ok(_) => initialized += 1,
                Err(e) => {
                    drop(our_guard);
                    participants[..initialized]
                        .iter_mut()
                        // Safety: initialized up to `initialized`
                        .for_each(|x| unsafe { x.assume_init_drop() });

                    return Err(e);
                }
            }
        }

        // should probably keep this until all init is complete
        std::mem::forget(our_guard);
    }

    // Safety: it is now initialized
    let _ = unsafe { place.assume_init_mut() };
    drop(place);

    // FIXME: how exactly to do an Arc out of this? Maybe an Arc<Box<RawSharedMemPipe>>, since we
    // cannot access ArcInner ... which does have a repr(c) but the layout would be version
    // dependent... maybe the custom arc crate with only strong counts?
    //
    // Or just give deref to SharedMemPipePtr and that's it, the ptr can be Arc'd

    // signal ready
    res.magic
        .store(0xcafebabe, std::sync::atomic::Ordering::SeqCst);

    res.2 = true;

    Ok(res)
}

pub struct SharedMemPipePtr(Option<NonNull<RawSharedMemPipe>>, NonZeroUsize, bool);

impl SharedMemPipePtr {
    fn close(&mut self) -> std::io::Result<()> {
        if let Some(ptr) = self.0.take() {
            let mut cleared_mutex = false;

            if self.2 {
                // go and unlock the first participant mutex, which had it's mutexguard forgotten
                // FIXME: easy no UB way would be to just lock the damn thing from outside, this is
                // just bad design
                let inner = unsafe { ptr.as_ref() };
                let first = &inner.participants[0];
                let first = unsafe { Pin::new_unchecked(first) };
                let stored = unsafe { *first.inner() };

                if stored == Some(std::process::id()) {
                    first.mutex().unlock();
                    cleared_mutex = true;
                }

                // does this make any sense, or should the structures be manuallydrop? we actually
                // can't do this before everyone else exits. failure to do proper shutdown might
                // leave some locked futexes in this area of memory which could be later on reused
                // by the normal allocator for example...
                //
                // solving this in the general and even current practical is quite difficult.
                // we could do refcounting, but that wont help in the case of everyone getting
                // killed... but that would be actually just fine, because then no one can suffer
                // from the futexes in their memory.
                //
                // TODO: refcounting is probably the way.
                // TODO: could look what effect does munmap to locked futexes.
                unsafe { ptr.as_ptr().drop_in_place() };
            }
            unsafe { nix::sys::mman::munmap(ptr.as_ptr().cast(), self.1.get())? };

            if !cleared_mutex {
                panic!("probably commited some crimes by destorying a locked mutex");
            }
        }
        Ok(())
    }

    fn ptr(&self) -> Option<NonNull<RawSharedMemPipe>> {
        self.0.clone()
    }
}

impl Drop for SharedMemPipePtr {
    fn drop(&mut self) {
        // FIXME: drop in place? but can we do it as we forgot the mutex guard to participants[0],
        // no we cannot
        let _ = self.close();
    }
}

impl std::ops::Deref for SharedMemPipePtr {
    type Target = RawSharedMemPipe;

    fn deref(&self) -> &Self::Target {
        if let Some(ptr) = self.0.clone() {
            unsafe { &ptr.as_ref() }
        } else {
            panic!("deref after close")
        }
    }
}

pub fn open_existing(path: &Path) -> std::io::Result<SharedMemPipePtr> {
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

    // use this on stack for panics until init is complete, then Arc it?
    let res = SharedMemPipePtr(Some(ptr.cast::<RawSharedMemPipe>()), size, false);

    let inner = res.ptr().unwrap();
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
            if magic.load(std::sync::atomic::Ordering::SeqCst) == 0xcafebabe {
                ready = true;
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        if !ready {
            panic!("creation did not complete in 1s");
        }
    }

    // only the owner sets the .2 = true on res, but even the owner should not do that
    Ok(res)
}
