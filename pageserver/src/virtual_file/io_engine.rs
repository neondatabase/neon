//! [`super::VirtualFile`] supports different IO engines.
//!
//! The [`IoEngineKind`] enum identifies them.
//!
//! The choice of IO engine is global.
//! Initialize using [`init`].
//!
//! Then use [`get`] and  [`super::OpenOptions`].

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
    Debug,
)]
#[strum(serialize_all = "kebab-case")]
pub enum IoEngineKind {
    StdFs,
    #[cfg(target_os = "linux")]
    TokioEpollUring,
}

static IO_ENGINE: once_cell::sync::OnceCell<IoEngineKind> = once_cell::sync::OnceCell::new();

#[cfg(not(test))]
pub(super) fn init(engine: IoEngineKind) {
    if IO_ENGINE.set(engine).is_err() {
        panic!("called twice");
    }
    crate::metrics::virtual_file_io_engine::KIND
        .with_label_values(&[&format!("{engine}")])
        .set(1);
}

pub(super) fn get() -> &'static IoEngineKind {
    #[cfg(test)]
    {
        let env_var_name = "NEON_PAGESERVER_UNIT_TEST_VIRTUAL_FILE_IOENGINE";
        IO_ENGINE.get_or_init(|| match std::env::var(env_var_name) {
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
        })
    }
    #[cfg(not(test))]
    IO_ENGINE.get().unwrap()
}

use std::os::unix::prelude::FileExt;

use super::FileGuard;

impl IoEngineKind {
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
            IoEngineKind::StdFs => {
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
            IoEngineKind::TokioEpollUring => {
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
