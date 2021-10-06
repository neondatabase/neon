use std::{
    convert::TryInto,
    io::{ErrorKind, Read},
    os::unix::prelude::AsRawFd,
    time::{Duration, Instant},
};

pub fn set_nonblocking(fd: &impl AsRawFd) -> std::io::Result<()> {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};

    let fd = fd.as_raw_fd();
    let flags_bits = fcntl(fd, FcntlArg::F_GETFL).unwrap();
    let mut flags = OFlag::from_bits(flags_bits).unwrap();
    flags.insert(OFlag::O_NONBLOCK);
    fcntl(fd, FcntlArg::F_SETFL(flags)).unwrap();
    Ok(())
}

pub struct TimeoutReader<'a, R> {
    pub timeout: Duration,
    pub reader: &'a mut R,
}

impl<R: Read + AsRawFd> std::io::Read for TimeoutReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut start_time_opt: Option<Instant> = None;
        loop {
            match self.reader.read(buf) {
                ok @ Ok(_) => return ok,
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {}
                err @ Err(_) => return err,
            }

            let timeout = if let Some(start_time) = start_time_opt {
                let elapsed = start_time.elapsed();
                match self.timeout.checked_sub(elapsed) {
                    Some(timeout) => timeout,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "read timed out",
                        ));
                    }
                }
            } else {
                start_time_opt = Some(Instant::now());
                self.timeout
            };

            use nix::{
                errno::Errno,
                poll::{poll, PollFd, PollFlags},
            };
            let mut poll_fd = PollFd::new(self.reader.as_raw_fd(), PollFlags::POLLIN);

            let millis: i32 = timeout.as_millis().try_into().unwrap_or(i32::MAX);

            match poll(std::slice::from_mut(&mut poll_fd), millis) {
                Ok(0) => {}
                Ok(n) => {
                    debug_assert!(n == 1);
                }
                Err(Errno::EINTR) => {}
                Err(e) => return Err(std::io::Error::from_raw_os_error(e as i32)),
            }
        }
    }
}

pub struct TimeoutWriter<'a, W: std::io::Write + AsRawFd> {
    pub timeout: Duration,
    pub writer: &'a mut W,
}

impl<W: std::io::Write + AsRawFd> std::io::Write for TimeoutWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut start_time_opt: Option<Instant> = None;
        loop {
            match self.writer.write(buf) {
                ok @ Ok(_) => return ok,
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {}
                err @ Err(_) => return err,
            }

            let timeout = if let Some(start_time) = start_time_opt {
                let elapsed = start_time.elapsed();
                match self.timeout.checked_sub(elapsed) {
                    Some(timeout) => timeout,
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "write timed out",
                        ));
                    }
                }
            } else {
                start_time_opt = Some(Instant::now());
                self.timeout
            };

            use nix::{
                errno::Errno,
                poll::{poll, PollFd, PollFlags},
            };
            let mut poll_fd = PollFd::new(self.writer.as_raw_fd(), PollFlags::POLLOUT);

            let millis: i32 = timeout.as_millis().try_into().unwrap_or(i32::MAX);

            match poll(std::slice::from_mut(&mut poll_fd), millis) {
                Ok(0) => {} // TODO want to check timeout before calling read again
                Ok(n) => {
                    debug_assert!(n == 1);
                }
                Err(Errno::EINTR) => {}
                Err(e) => return Err(std::io::Error::from_raw_os_error(e as i32)),
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
