use crate::open_existing;
use crate::OwnedResponder;

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
