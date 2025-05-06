//! Enum-dispatch to the `OpenOptions` type of the respective [`super::IoEngineKind`];

use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::io_engine::IoEngine;

#[derive(Debug, Clone)]
pub struct OpenOptions {
    write: bool,
    inner: Inner,
}
#[derive(Debug, Clone)]
enum Inner {
    StdFs(std::fs::OpenOptions),
    #[cfg(target_os = "linux")]
    TokioEpollUring(tokio_epoll_uring::ops::open_at::OpenOptions),
}

impl Default for OpenOptions {
    fn default() -> Self {
        let inner = match super::io_engine::get() {
            IoEngine::NotSet => panic!("io engine not set"),
            IoEngine::StdFs => Inner::StdFs(std::fs::OpenOptions::new()),
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                Inner::TokioEpollUring(tokio_epoll_uring::ops::open_at::OpenOptions::new())
            }
        };
        Self {
            write: false,
            inner,
        }
    }
}

impl OpenOptions {
    pub fn new() -> OpenOptions {
        Self::default()
    }

    pub(super) fn is_write(&self) -> bool {
        self.write
    }

    pub fn read(mut self, read: bool) -> Self {
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.read(read);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.read(read);
            }
        }
        self
    }

    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.write(write);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.write(write);
            }
        }
        self
    }

    pub fn create(mut self, create: bool) -> Self {
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.create(create);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.create(create);
            }
        }
        self
    }

    pub fn create_new(mut self, create_new: bool) -> Self {
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.create_new(create_new);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.create_new(create_new);
            }
        }
        self
    }

    pub fn truncate(mut self, truncate: bool) -> Self {
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.truncate(truncate);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.truncate(truncate);
            }
        }
        self
    }

    /// Don't use, `O_APPEND` is not supported.
    pub fn append(&mut self, _append: bool) {
        super::io_engine::panic_operation_must_be_idempotent();
    }

    pub(in crate::virtual_file) async fn open(&self, path: &Path) -> std::io::Result<OwnedFd> {
        match &self.inner {
            Inner::StdFs(x) => x.open(path).map(|file| file.into()),
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let system = super::io_engine::tokio_epoll_uring_ext::thread_local_system().await;
                let (_, res) = super::io_engine::retry_ecanceled_once((), |()| async {
                    let res = system.open(path, x).await;
                    ((), res)
                })
                .await;
                res.map_err(super::io_engine::epoll_uring_error_to_std)
            }
        }
    }

    pub fn mode(mut self, mode: u32) -> Self {
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.mode(mode);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.mode(mode);
            }
        }
        self
    }

    pub fn custom_flags(mut self, flags: i32) -> Self {
        if flags & nix::libc::O_APPEND != 0 {
            super::io_engine::panic_operation_must_be_idempotent();
        }
        match &mut self.inner {
            Inner::StdFs(x) => {
                let _ = x.custom_flags(flags);
            }
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(x) => {
                let _ = x.custom_flags(flags);
            }
        }
        self
    }
}
