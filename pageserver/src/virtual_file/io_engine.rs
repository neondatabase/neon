//! [`super::VirtualFile`] supports different IO engines.
//!
//! The [`IoEngineKind`] enum identifies them.
//!
//! The choice of IO engine is global.
//! Initialize using [`init`].
//!
//! Then use [`get`] and  [`super::OpenOptions`].

pub(crate) use super::api::IoEngineKind;
#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum IoEngine {
    NotSet,
    StdFs,
    #[cfg(target_os = "linux")]
    TokioEpollUring,
}

impl From<IoEngineKind> for IoEngine {
    fn from(value: IoEngineKind) -> Self {
        match value {
            IoEngineKind::StdFs => IoEngine::StdFs,
            #[cfg(target_os = "linux")]
            IoEngineKind::TokioEpollUring => IoEngine::TokioEpollUring,
        }
    }
}

impl TryFrom<u8> for IoEngine {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            v if v == (IoEngine::NotSet as u8) => IoEngine::NotSet,
            v if v == (IoEngine::StdFs as u8) => IoEngine::StdFs,
            #[cfg(target_os = "linux")]
            v if v == (IoEngine::TokioEpollUring as u8) => IoEngine::TokioEpollUring,
            x => return Err(x),
        })
    }
}

static IO_ENGINE: AtomicU8 = AtomicU8::new(IoEngine::NotSet as u8);

pub(crate) fn set(engine_kind: IoEngineKind) {
    let engine: IoEngine = engine_kind.into();
    IO_ENGINE.store(engine as u8, std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(test))]
    {
        let metric = &crate::metrics::virtual_file_io_engine::KIND;
        metric.reset();
        metric
            .with_label_values(&[&format!("{engine_kind}")])
            .set(1);
    }
}

#[cfg(not(test))]
pub(super) fn init(engine_kind: IoEngineKind) {
    set(engine_kind);
}

/// TODO: longer-term this should only be visible to VirtualFile
pub(crate) fn get() -> IoEngine {
    let cur = IoEngine::try_from(IO_ENGINE.load(Ordering::Relaxed)).unwrap();
    if cfg!(test) {
        let env_var_name = "NEON_PAGESERVER_UNIT_TEST_VIRTUAL_FILE_IOENGINE";
        match cur {
            IoEngine::NotSet => {
                let kind = match std::env::var(env_var_name) {
                    Ok(v) => match v.parse::<IoEngineKind>() {
                        Ok(engine_kind) => engine_kind,
                        Err(e) => {
                            panic!("invalid VirtualFile io engine for env var {env_var_name}: {e:#}: {v:?}")
                        }
                    },
                    Err(std::env::VarError::NotPresent) => {
                        crate::config::defaults::DEFAULT_VIRTUAL_FILE_IO_ENGINE
                            .parse()
                            .unwrap()
                    }
                    Err(std::env::VarError::NotUnicode(_)) => {
                        panic!("env var {env_var_name} is not unicode");
                    }
                };
                self::set(kind);
                self::get()
            }
            x => x,
        }
    } else {
        cur
    }
}

use std::{
    os::unix::prelude::FileExt,
    sync::atomic::{AtomicU8, Ordering},
};

use super::FileGuard;

impl IoEngine {
    pub(super) async fn read_at<B>(
        &self,
        file_guard: FileGuard,
        offset: u64,
        mut buf: B,
    ) -> ((FileGuard, B), std::io::Result<usize>)
    where
        B: tokio_epoll_uring::BoundedBufMut + Send,
    {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                // SAFETY: `dst` only lives at most as long as this match arm, during which buf remains valid memory.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(buf.stable_mut_ptr(), buf.bytes_total())
                };
                let res = file_guard.with_std_file(|std_file| std_file.read_at(dst, offset));
                if let Ok(nbytes) = &res {
                    assert!(*nbytes <= buf.bytes_total());
                    // SAFETY: see above assertion
                    unsafe {
                        buf.set_init(*nbytes);
                    }
                }
                #[allow(dropping_references)]
                drop(dst);
                ((file_guard, buf), res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring::thread_local_system().await;
                let (resources, res) = system.read(file_guard, offset, buf).await;
                (
                    resources,
                    res.map_err(|e| match e {
                        tokio_epoll_uring::Error::Op(e) => e,
                        tokio_epoll_uring::Error::System(system) => {
                            std::io::Error::new(std::io::ErrorKind::Other, system)
                        }
                    }),
                )
            }
        }
    }
}
