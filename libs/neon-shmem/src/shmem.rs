//! Dynamically resizable contiguous chunk of shared memory

use std::num::NonZeroUsize;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use nix::errno::Errno;
use nix::sys::mman::MapFlags;
use nix::sys::mman::ProtFlags;
use nix::sys::mman::mmap as nix_mmap;
use nix::sys::mman::munmap as nix_munmap;
use nix::unistd::ftruncate as nix_ftruncate;

/// `ShmemHandle` represents a shared memory area that can be shared by processes over `fork()`.
/// Unlike shared memory allocated by Postgres, this area is resizable, up to `max_size` that's
/// specified at creation.
///
/// The area is backed by an anonymous file created with `memfd_create()`. The full address space for
/// `max_size` is reserved up-front with `mmap()`, but whenever you call [`ShmemHandle::set_size`],
/// the underlying file is resized. Do not access the area beyond the current size. Currently, that
/// will cause the file to be expanded, but we might use `mprotect()` etc. to enforce that in the
/// future.
pub struct ShmemHandle {
    /// memfd file descriptor
    fd: OwnedFd,

    max_size: usize,

    // Pointer to the beginning of the shared memory area. The header is stored there.
    shared_ptr: NonNull<SharedStruct>,

    // Pointer to the beginning of the user data
    pub data_ptr: NonNull<u8>,
}

/// This is stored at the beginning in the shared memory area.
struct SharedStruct {
    max_size: usize,

    /// Current size of the backing file. The high-order bit is used for the [`RESIZE_IN_PROGRESS`] flag.
    current_size: AtomicUsize,
}

const RESIZE_IN_PROGRESS: usize = 1 << 63;

const HEADER_SIZE: usize = std::mem::size_of::<SharedStruct>();

/// Error type returned by the [`ShmemHandle`] functions.
#[derive(thiserror::Error, Debug)]
#[error("{msg}: {errno}")]
pub struct Error {
    pub msg: String,
    pub errno: Errno,
}

impl Error {
    fn new(msg: &str, errno: Errno) -> Self {
        Self {
            msg: msg.to_string(),
            errno,
        }
    }
}

impl ShmemHandle {
    /// Create a new shared memory area. To communicate between processes, the processes need to be
    /// `fork()`'d after calling this, so that the `ShmemHandle` is inherited by all processes.
    ///
    /// If the `ShmemHandle` is dropped, the memory is unmapped from the current process. Other
    /// processes can continue using it, however.
    pub fn new(name: &str, initial_size: usize, max_size: usize) -> Result<Self, Error> {
        // create the backing anonymous file.
        let fd = create_backing_file(name)?;

        Self::new_with_fd(fd, initial_size, max_size)
    }

    fn new_with_fd(fd: OwnedFd, initial_size: usize, max_size: usize) -> Result<Self, Error> {
        // We reserve the high-order bit for the `RESIZE_IN_PROGRESS` flag, and the actual size
        // is a little larger than this because of the SharedStruct header. Make the upper limit
        // somewhat smaller than that, because with anything close to that, you'll run out of
        // memory anyway.
        assert!(max_size < 1 << 48, "max size {max_size} too large");

        assert!(
            initial_size <= max_size,
            "initial size {initial_size} larger than max size {max_size}"
        );

        // The actual initial / max size is the one given by the caller, plus the size of
        // 'SharedStruct'.
        let initial_size = HEADER_SIZE + initial_size;
        let max_size = NonZeroUsize::new(HEADER_SIZE + max_size).unwrap();

        // Reserve address space for it with mmap
        //
        // TODO: Use MAP_HUGETLB if possible
        let start_ptr = unsafe {
            nix_mmap(
                None,
                max_size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                &fd,
                0,
            )
        }
        .map_err(|e| Error::new("mmap failed", e))?;

        // Reserve space for the initial size
        enlarge_file(fd.as_fd(), initial_size as u64)?;

        // Initialize the header
        let shared: NonNull<SharedStruct> = start_ptr.cast();
        unsafe {
            shared.write(SharedStruct {
                max_size: max_size.into(),
                current_size: AtomicUsize::new(initial_size),
            });
        }

        // The user data begins after the header
        let data_ptr = unsafe { start_ptr.cast().add(HEADER_SIZE) };

        Ok(Self {
            fd,
            max_size: max_size.into(),
            shared_ptr: shared,
            data_ptr,
        })
    }

    // return reference to the header
    fn shared(&self) -> &SharedStruct {
        unsafe { self.shared_ptr.as_ref() }
    }

    /// Resize the shared memory area. `new_size` must not be larger than the `max_size` specified
    /// when creating the area.
    ///
    /// This may only be called from one process/thread concurrently. We detect that case
    /// and return an [`shmem::Error`](Error).
    pub fn set_size(&self, new_size: usize) -> Result<(), Error> {
        let new_size = new_size + HEADER_SIZE;
        let shared = self.shared();

        assert!(
            new_size <= self.max_size,
            "new size ({new_size}) is greater than max size ({})",
            self.max_size
        );

        assert_eq!(self.max_size, shared.max_size);

        // Lock the area by setting the bit in `current_size`
        //
        // Ordering::Relaxed would probably be sufficient here, as we don't access any other memory
        // and the `posix_fallocate`/`ftruncate` call is surely a synchronization point anyway. But
        // since this is not performance-critical, better safe than sorry.
        let mut old_size = shared.current_size.load(Ordering::Acquire);
        loop {
            if (old_size & RESIZE_IN_PROGRESS) != 0 {
                return Err(Error::new(
                    "concurrent resize detected",
                    Errno::UnknownErrno,
                ));
            }
            match shared.current_size.compare_exchange(
                old_size,
                new_size,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => old_size = x,
            }
        }

        // Ok, we got the lock.
        //
        // NB: If anything goes wrong, we *must* clear the bit!
        let result = {
            use std::cmp::Ordering::{Equal, Greater, Less};
            match new_size.cmp(&old_size) {
                Less => nix_ftruncate(&self.fd, new_size as i64)
                    .map_err(|e| Error::new("could not shrink shmem segment, ftruncate failed", e)),
                Equal => Ok(()),
                Greater => enlarge_file(self.fd.as_fd(), new_size as u64),
            }
        };

        // Unlock
        shared.current_size.store(
            if result.is_ok() { new_size } else { old_size },
            Ordering::Release,
        );

        result
    }

    /// Returns the current user-visible size of the shared memory segment.
    ///
    /// NOTE: a concurrent [`ShmemHandle::set_size()`] call can change the size at any time.
    /// It is the caller's responsibility not to access the area beyond the current size.
    pub fn current_size(&self) -> usize {
        let total_current_size =
            self.shared().current_size.load(Ordering::Relaxed) & !RESIZE_IN_PROGRESS;
        total_current_size - HEADER_SIZE
    }
}

impl Drop for ShmemHandle {
    fn drop(&mut self) {
        // SAFETY: The pointer was obtained from mmap() with the given size.
        // We unmap the entire region.
        let _ = unsafe { nix_munmap(self.shared_ptr.cast(), self.max_size) };
        // The fd is dropped automatically by OwnedFd.
    }
}

/// Create a "backing file" for the shared memory area. On Linux, use `memfd_create()`, to create an
/// anonymous in-memory file. One macos, fall back to a regular file. That's good enough for
/// development and testing, but in production we want the file to stay in memory.
///
/// Disable unused variables warnings because `name` is unused in the macos path.
#[allow(unused_variables)]
fn create_backing_file(name: &str) -> Result<OwnedFd, Error> {
    #[cfg(not(target_os = "macos"))]
    {
        nix::sys::memfd::memfd_create(name, nix::sys::memfd::MFdFlags::empty())
            .map_err(|e| Error::new("memfd_create failed", e))
    }
    #[cfg(target_os = "macos")]
    {
        let file = tempfile::tempfile().map_err(|e| {
            Error::new(
                "could not create temporary file to back shmem area",
                nix::errno::Errno::from_raw(e.raw_os_error().unwrap_or(0)),
            )
        })?;
        Ok(OwnedFd::from(file))
    }
}

fn enlarge_file(fd: BorrowedFd, size: u64) -> Result<(), Error> {
    // Use posix_fallocate() to enlarge the file. It reserves the space correctly, so that
    // we don't get a segfault later when trying to actually use it.
    #[cfg(not(target_os = "macos"))]
    {
        nix::fcntl::posix_fallocate(fd, 0, size as i64)
            .map_err(|e| Error::new("could not grow shmem segment, posix_fallocate failed", e))
    }
    // As a fallback on macos, which doesn't have posix_fallocate, use plain 'fallocate'
    #[cfg(target_os = "macos")]
    {
        nix::unistd::ftruncate(fd, size as i64)
            .map_err(|e| Error::new("could not grow shmem segment, ftruncate failed", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nix::unistd::ForkResult;
    use std::ops::Range;

    /// check that all bytes in given range have the expected value.
    fn assert_range(ptr: *const u8, expected: u8, range: Range<usize>) {
        for i in range {
            let b = unsafe { *(ptr.add(i)) };
            assert_eq!(expected, b, "unexpected byte at offset {i}");
        }
    }

    /// Write 'b' to all bytes in the given range
    fn write_range(ptr: *mut u8, b: u8, range: Range<usize>) {
        unsafe { std::ptr::write_bytes(ptr.add(range.start), b, range.end - range.start) };
    }

    // simple single-process test of growing and shrinking
    #[test]
    fn test_shmem_resize() -> Result<(), Error> {
        let max_size = 1024 * 1024;
        let init_struct = ShmemHandle::new("test_shmem_resize", 0, max_size)?;

        assert_eq!(init_struct.current_size(), 0);

        // Initial grow
        let size1 = 10000;
        init_struct.set_size(size1).unwrap();
        assert_eq!(init_struct.current_size(), size1);

        // Write some data
        let data_ptr = init_struct.data_ptr.as_ptr();
        write_range(data_ptr, 0xAA, 0..size1);
        assert_range(data_ptr, 0xAA, 0..size1);

        // Shrink
        let size2 = 5000;
        init_struct.set_size(size2).unwrap();
        assert_eq!(init_struct.current_size(), size2);

        // Grow again
        let size3 = 20000;
        init_struct.set_size(size3).unwrap();
        assert_eq!(init_struct.current_size(), size3);

        // Try to read it. The area that was shrunk and grown again should read as all zeros now
        assert_range(data_ptr, 0xAA, 0..5000);
        assert_range(data_ptr, 0, 5000..size1);

        // Try to grow beyond max_size
        //let size4 = max_size + 1;
        //assert!(init_struct.set_size(size4).is_err());

        // Dropping init_struct should unmap the memory
        drop(init_struct);

        Ok(())
    }

    /// This is used in tests to coordinate between test processes. It's like `std::sync::Barrier`,
    /// but is stored in the shared memory area and works across processes. It's implemented by
    /// polling, because e.g. standard rust mutexes are not guaranteed to work across processes.
    struct SimpleBarrier {
        num_procs: usize,
        count: AtomicUsize,
    }

    impl SimpleBarrier {
        unsafe fn init(ptr: *mut SimpleBarrier, num_procs: usize) {
            unsafe {
                *ptr = SimpleBarrier {
                    num_procs,
                    count: AtomicUsize::new(0),
                }
            }
        }

        pub fn wait(&self) {
            let old = self.count.fetch_add(1, Ordering::Relaxed);

            let generation = old / self.num_procs;

            let mut current = old + 1;
            while current < (generation + 1) * self.num_procs {
                std::thread::sleep(std::time::Duration::from_millis(10));
                current = self.count.load(Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn test_multi_process() {
        // Initialize
        let max_size = 1_000_000_000_000;
        let init_struct = ShmemHandle::new("test_multi_process", 0, max_size).unwrap();
        let ptr = init_struct.data_ptr.as_ptr();

        // Store the SimpleBarrier in the first 1k of the area.
        init_struct.set_size(10000).unwrap();
        let barrier_ptr: *mut SimpleBarrier = unsafe {
            ptr.add(ptr.align_offset(std::mem::align_of::<SimpleBarrier>()))
                .cast()
        };
        unsafe { SimpleBarrier::init(barrier_ptr, 2) };
        let barrier = unsafe { barrier_ptr.as_ref().unwrap() };

        // Fork another test process. The code after this runs in both processes concurrently.
        let fork_result = unsafe { nix::unistd::fork().unwrap() };

        // In the parent, fill bytes between 1000..2000. In the child, between 2000..3000
        if fork_result.is_parent() {
            write_range(ptr, 0xAA, 1000..2000);
        } else {
            write_range(ptr, 0xBB, 2000..3000);
        }
        barrier.wait();
        // Verify the contents. (in both processes)
        assert_range(ptr, 0xAA, 1000..2000);
        assert_range(ptr, 0xBB, 2000..3000);

        // Grow, from the child this time
        let size = 10_000_000;
        if !fork_result.is_parent() {
            init_struct.set_size(size).unwrap();
        }
        barrier.wait();

        // make some writes at the end
        if fork_result.is_parent() {
            write_range(ptr, 0xAA, (size - 10)..size);
        } else {
            write_range(ptr, 0xBB, (size - 20)..(size - 10));
        }
        barrier.wait();

        // Verify the contents. (This runs in both processes)
        assert_range(ptr, 0, (size - 1000)..(size - 20));
        assert_range(ptr, 0xBB, (size - 20)..(size - 10));
        assert_range(ptr, 0xAA, (size - 10)..size);

        if let ForkResult::Parent { child } = fork_result {
            nix::sys::wait::waitpid(child, None).unwrap();
        }
    }
}
