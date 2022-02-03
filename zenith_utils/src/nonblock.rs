use nix::fcntl::{fcntl, OFlag, F_GETFL, F_SETFL};
use std::os::unix::io::RawFd;

/// Put a file descriptor into non-blocking mode
pub fn set_nonblock(fd: RawFd) -> Result<(), std::io::Error> {
    let bits = fcntl(fd, F_GETFL)?;

    // Safety: If F_GETFL returns some unknown bits, they should be valid
    // for passing back to F_SETFL, too. If we left them out, the F_SETFL
    // would effectively clear them, which is not what we want.
    let mut flags = unsafe { OFlag::from_bits_unchecked(bits) };
    flags |= OFlag::O_NONBLOCK;

    fcntl(fd, F_SETFL(flags))?;

    Ok(())
}
