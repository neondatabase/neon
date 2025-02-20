//! Linux-specific socket ioctls.
//!
//! <https://elixir.bootlin.com/linux/v6.1.128/source/include/uapi/linux/sockios.h#L25-L27>

use std::{
    io,
    mem::MaybeUninit,
    os::{fd::RawFd, raw::c_int},
};

use nix::libc::{FIONREAD, TIOCOUTQ};

unsafe fn do_ioctl(socket_fd: RawFd, cmd: nix::libc::Ioctl) -> io::Result<c_int> {
    let mut inq: MaybeUninit<c_int> = MaybeUninit::uninit();
    // SAFETY: encapsulating fn is unsafe, we require `socket_fd` to be a valid file descriptor
    unsafe {
        let err = nix::libc::ioctl(socket_fd, cmd, inq.as_mut_ptr());
        if err == 0 {
            Ok(inq.assume_init())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// # Safety
///
/// Caller must ensure that `socket_fd` is a valid TCP socket file descriptor.
pub unsafe fn inq(socket_fd: RawFd) -> io::Result<c_int> {
    // SAFETY: encapsulating fn is unsafe
    unsafe { do_ioctl(socket_fd, FIONREAD) }
}

/// # Safety
///
/// Caller must ensure that `socket_fd` is a valid TCP socket file descriptor.
pub unsafe fn outq(socket_fd: RawFd) -> io::Result<c_int> {
    // SAFETY: encapsulating fn is unsafe
    unsafe { do_ioctl(socket_fd, TIOCOUTQ) }
}
