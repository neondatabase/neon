//! Initialization functions. These are executed in the postmaster process,
//! at different stages of server startup.
//!
//!
//! Communicator initialization steps:
//!
//! 1. At postmaster startup, before shared memory is allocated,
//!    rcommunicator_shmem_size() is called to get the amount of
//!    shared memory that this module needs.
//!
//! 2. Later, after the shared memory has been allocated,
//!    rcommunicator_shmem_init() is called to initialize the shmem
//!    area.
//!
//! Per process initialization:
//!
//! When a backend process starts up, it calls rcommunicator_backend_init().
//! In the communicator worker process, other functions are called, see
//! `worker_process` module.

use std::ffi::c_int;
use std::mem;
use std::mem::MaybeUninit;
use std::os::fd::OwnedFd;

use crate::backend_comms::NeonIORequestSlot;
use crate::integrated_cache::IntegratedCacheInitStruct;

/// This struct is created in the postmaster process, and inherited to
/// the communicator process and all backend processes through fork()
#[repr(C)]
pub struct CommunicatorInitStruct {
    pub submission_pipe_read_fd: OwnedFd,
    pub submission_pipe_write_fd: OwnedFd,

    // Shared memory data structures
    pub num_neon_request_slots: u32,

    pub neon_request_slots: &'static [NeonIORequestSlot],

    pub integrated_cache_init_struct: IntegratedCacheInitStruct<'static>,
}

impl std::fmt::Debug for CommunicatorInitStruct {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        fmt.debug_struct("CommunicatorInitStruct")
            .field("submission_pipe_read_fd", &self.submission_pipe_read_fd)
            .field("submission_pipe_write_fd", &self.submission_pipe_write_fd)
            .field("num_neon_request_slots", &self.num_neon_request_slots)
            .field("neon_request_slots length", &self.neon_request_slots.len())
            .finish()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_shmem_size(num_neon_request_slots: u32) -> u64 {
    let mut size = 0;

    size += mem::size_of::<NeonIORequestSlot>() * num_neon_request_slots as usize;

    // For integrated_cache's Allocator. TODO: make this adjustable
    size += IntegratedCacheInitStruct::shmem_size();

    size as u64
}

/// Initialize the shared memory segment. Returns a backend-private
/// struct, which will be inherited by backend processes through fork
#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_shmem_init(
    submission_pipe_read_fd: c_int,
    submission_pipe_write_fd: c_int,
    num_neon_request_slots: u32,
    shmem_area_ptr: *mut MaybeUninit<u8>,
    shmem_area_len: u64,
    initial_file_cache_size: u64,
    max_file_cache_size: u64,
) -> &'static mut CommunicatorInitStruct {
    let shmem_area: &'static mut [MaybeUninit<u8>] =
        unsafe { std::slice::from_raw_parts_mut(shmem_area_ptr, shmem_area_len as usize) };

    let (neon_request_slots, remaining_area) =
        alloc_array_from_slice::<NeonIORequestSlot>(shmem_area, num_neon_request_slots as usize);

    for slot in neon_request_slots.iter_mut() {
        slot.write(NeonIORequestSlot::default());
    }

    // 'neon_request_slots' is initialized now. (MaybeUninit::slice_assume_init_mut() is nightly-only
    // as of this writing.)
    let neon_request_slots = unsafe {
        std::mem::transmute::<&mut [MaybeUninit<NeonIORequestSlot>], &mut [NeonIORequestSlot]>(
            neon_request_slots,
        )
    };

    // Give the rest of the area to the integrated cache
    let integrated_cache_init_struct = IntegratedCacheInitStruct::shmem_init(
        remaining_area,
        initial_file_cache_size,
        max_file_cache_size,
    );

    let (submission_pipe_read_fd, submission_pipe_write_fd) = unsafe {
        use std::os::fd::FromRawFd;
        (
            OwnedFd::from_raw_fd(submission_pipe_read_fd),
            OwnedFd::from_raw_fd(submission_pipe_write_fd),
        )
    };

    let cis: &'static mut CommunicatorInitStruct = Box::leak(Box::new(CommunicatorInitStruct {
        submission_pipe_read_fd,
        submission_pipe_write_fd,

        num_neon_request_slots,
        neon_request_slots,

        integrated_cache_init_struct,
    }));

    cis
}

pub fn alloc_from_slice<T>(
    area: &mut [MaybeUninit<u8>],
) -> (&mut MaybeUninit<T>, &mut [MaybeUninit<u8>]) {
    let layout = std::alloc::Layout::new::<T>();

    let area_start = area.as_mut_ptr();

    // pad to satisfy alignment requirements
    let padding = area_start.align_offset(layout.align());
    if padding + layout.size() > area.len() {
        panic!("out of memory");
    }
    let area = &mut area[padding..];
    let (result_area, remain) = area.split_at_mut(layout.size());

    let result_ptr: *mut MaybeUninit<T> = result_area.as_mut_ptr().cast();
    let result = unsafe { result_ptr.as_mut().unwrap() };

    (result, remain)
}

pub fn alloc_array_from_slice<T>(
    area: &mut [MaybeUninit<u8>],
    len: usize,
) -> (&mut [MaybeUninit<T>], &mut [MaybeUninit<u8>]) {
    let layout = std::alloc::Layout::new::<T>();

    let area_start = area.as_mut_ptr();

    // pad to satisfy alignment requirements
    let padding = area_start.align_offset(layout.align());
    if padding + layout.size() * len > area.len() {
        panic!("out of memory");
    }
    let area = &mut area[padding..];
    let (result_area, remain) = area.split_at_mut(layout.size() * len);

    let result_ptr: *mut MaybeUninit<T> = result_area.as_mut_ptr().cast();
    let result = unsafe { std::slice::from_raw_parts_mut(result_ptr.as_mut().unwrap(), len) };

    (result, remain)
}
