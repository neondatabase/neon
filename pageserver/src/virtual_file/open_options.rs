//! Enum-dispatch to the `OpenOptions` type of the respective [`super::IoEngineKind`];

use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use super::io_engine::IoEngine;

#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// We keep a copy of the write() flag we pass to the `inner`` `OptionOptions`
    /// to support [`Self::is_write`].
    write: bool,
    /// We don't expose + pass through a raw `custom_flags()` style API.
    /// The only custom flag we support is `O_DIRECT`, which we track here
    /// and map to `custom_flags()` in the [`Self::open`] method.
    direct: bool,
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
            direct: false,
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

    pub(super) fn is_direct(&self) -> bool {
        self.direct
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
        #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
        let mut custom_flags = 0;
        if self.direct {
            #[cfg(target_os = "linux")]
            {
                custom_flags |= nix::libc::O_DIRECT;
            }
            #[cfg(not(target_os = "linux"))]
            {
                // Other platforms may be used for development but don't necessarily have a 1:1 equivalent to Linux's O_DIRECT (macOS!).
                // Just don't set the flag; to catch alignment bugs typical for O_DIRECT,
                // we have a runtime validation layer inside `VirtualFile::write_at` and `VirtualFile::read_at`.
                static WARNING: std::sync::Once = std::sync::Once::new();
                WARNING.call_once(|| {
                    let span = tracing::info_span!(parent: None, "open_options");
                    let _enter = span.enter();
                    tracing::warn!("your platform is not a supported production platform, ignoing request for O_DIRECT; this could hide alignment bugs; this warning is logged once per process");
                });
            }
        }

        match self.inner.clone() {
            Inner::StdFs(mut x) => x
                .custom_flags(custom_flags)
                .open(path)
                .map(|file| file.into()),
            #[cfg(target_os = "linux")]
            Inner::TokioEpollUring(mut x) => {
                x.custom_flags(custom_flags);
                let system = super::io_engine::tokio_epoll_uring_ext::thread_local_system().await;
                let (_, res) = super::io_engine::retry_ecanceled_once((), |()| async {
                    let res = system.open(path, &x).await;
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

    pub fn direct(mut self, direct: bool) -> Self {
        self.direct = direct;
        self
    }
}
