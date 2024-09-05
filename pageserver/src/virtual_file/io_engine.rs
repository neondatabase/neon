//! [`super::VirtualFile`] supports different IO engines.
//!
//! The [`IoEngineKind`] enum identifies them.
//!
//! The choice of IO engine is global.
//! Initialize using [`init`].
//!
//! Then use [`get`] and  [`super::OpenOptions`].
//!
//!

#[cfg(target_os = "linux")]
pub(super) mod tokio_epoll_uring_ext;

use tokio_epoll_uring::IoBuf;
use tracing::Instrument;

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

/// Longer-term, this API should only be used by [`super::VirtualFile`].
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
                        #[cfg(target_os = "linux")]
                        {
                            IoEngineKind::TokioEpollUring
                        }
                        #[cfg(not(target_os = "linux"))]
                        {
                            IoEngineKind::StdFs
                        }
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

use super::{
    owned_buffers_io::{io_buf_ext::FullSlice, slice::SliceMutExt},
    FileGuard, Metadata,
};

#[cfg(target_os = "linux")]
fn epoll_uring_error_to_std(e: tokio_epoll_uring::Error<std::io::Error>) -> std::io::Error {
    match e {
        tokio_epoll_uring::Error::Op(e) => e,
        tokio_epoll_uring::Error::System(system) => {
            std::io::Error::new(std::io::ErrorKind::Other, system)
        }
    }
}

impl IoEngine {
    pub(super) async fn read_at<Buf>(
        &self,
        file_guard: FileGuard,
        offset: u64,
        mut slice: tokio_epoll_uring::Slice<Buf>,
    ) -> (
        (FileGuard, tokio_epoll_uring::Slice<Buf>),
        std::io::Result<usize>,
    )
    where
        Buf: tokio_epoll_uring::IoBufMut + Send,
    {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let rust_slice = slice.as_mut_rust_slice_full_zeroed();
                let res = file_guard.with_std_file(|std_file| std_file.read_at(rust_slice, offset));
                ((file_guard, slice), res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring_ext::thread_local_system().await;
                let (resources, res) = system.read(file_guard, offset, slice).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }
    pub(super) async fn sync_all(&self, file_guard: FileGuard) -> (FileGuard, std::io::Result<()>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let res = file_guard.with_std_file(|std_file| std_file.sync_all());
                (file_guard, res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring_ext::thread_local_system().await;
                let (resources, res) = system.fsync(file_guard).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }
    pub(super) async fn sync_data(
        &self,
        file_guard: FileGuard,
    ) -> (FileGuard, std::io::Result<()>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let res = file_guard.with_std_file(|std_file| std_file.sync_data());
                (file_guard, res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring_ext::thread_local_system().await;
                let (resources, res) = system.fdatasync(file_guard).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }
    pub(super) async fn metadata(
        &self,
        file_guard: FileGuard,
    ) -> (FileGuard, std::io::Result<Metadata>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let res =
                    file_guard.with_std_file(|std_file| std_file.metadata().map(Metadata::from));
                (file_guard, res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring_ext::thread_local_system().await;
                let (resources, res) = system.statx(file_guard).await;
                (
                    resources,
                    res.map_err(epoll_uring_error_to_std).map(Metadata::from),
                )
            }
        }
    }
    pub(super) async fn write_at<B: IoBuf + Send>(
        &self,
        file_guard: FileGuard,
        offset: u64,
        buf: FullSlice<B>,
    ) -> ((FileGuard, FullSlice<B>), std::io::Result<usize>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let result = file_guard.with_std_file(|std_file| std_file.write_at(&buf, offset));
                ((file_guard, buf), result)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring_ext::thread_local_system().await;
                let ((file_guard, slice), res) =
                    system.write(file_guard, offset, buf.into_raw_slice()).await;
                (
                    (file_guard, FullSlice::must_new(slice)),
                    res.map_err(epoll_uring_error_to_std),
                )
            }
        }
    }

    /// If we switch a user of [`tokio::fs`] to use [`super::io_engine`],
    /// they'd start blocking the executor thread if [`IoEngine::StdFs`] is configured
    /// whereas before the switch to [`super::io_engine`], that wasn't the case.
    /// This method helps avoid such a regression.
    ///
    /// Panics if the `spawn_blocking` fails, see [`tokio::task::JoinError`] for reasons why that can happen.
    pub(crate) async fn spawn_blocking_and_block_on_if_std<Fut, R>(&self, work: Fut) -> R
    where
        Fut: 'static + Send + std::future::Future<Output = R>,
        R: 'static + Send,
    {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let span = tracing::info_span!("spawn_blocking_block_on_if_std");
                tokio::task::spawn_blocking({
                    move || tokio::runtime::Handle::current().block_on(work.instrument(span))
                })
                .await
                .expect("failed to join blocking code most likely it panicked, panicking as well")
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => work.await,
        }
    }
}

pub enum FeatureTestResult {
    PlatformPreferred(IoEngineKind),
    Worse {
        engine: IoEngineKind,
        remark: String,
    },
}

impl FeatureTestResult {
    #[cfg(target_os = "linux")]
    const PLATFORM_PREFERRED: IoEngineKind = IoEngineKind::TokioEpollUring;
    #[cfg(not(target_os = "linux"))]
    const PLATFORM_PREFERRED: IoEngineKind = IoEngineKind::StdFs;
}

impl From<FeatureTestResult> for IoEngineKind {
    fn from(val: FeatureTestResult) -> Self {
        match val {
            FeatureTestResult::PlatformPreferred(e) => e,
            FeatureTestResult::Worse { engine, .. } => engine,
        }
    }
}

/// Somewhat costly under the hood, do only once.
/// Panics if we can't set up the feature test.
pub fn feature_test() -> anyhow::Result<FeatureTestResult> {
    std::thread::spawn(|| {

        #[cfg(not(target_os = "linux"))]
        {
            Ok(FeatureTestResult::PlatformPreferred(
                FeatureTestResult::PLATFORM_PREFERRED,
            ))
        }
        #[cfg(target_os = "linux")]
        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            Ok(match rt.block_on(tokio_epoll_uring::System::launch()) {
                Ok(_) => FeatureTestResult::PlatformPreferred({
                    assert!(matches!(
                        IoEngineKind::TokioEpollUring,
                        FeatureTestResult::PLATFORM_PREFERRED
                    ));
                    FeatureTestResult::PLATFORM_PREFERRED
                }),
                Err(tokio_epoll_uring::LaunchResult::IoUringBuild(e)) => {
                    let remark = match e.raw_os_error() {
                        Some(nix::libc::EPERM) => {
                            // fall back
                            "creating tokio-epoll-uring fails with EPERM, assuming it's admin-disabled "
                                .to_string()
                        }
                    Some(nix::libc::EFAULT) => {
                            // fail feature test
                            anyhow::bail!(
                                "creating tokio-epoll-uring fails with EFAULT, might have corrupted memory"
                            );
                        }
                        Some(_) | None => {
                            // fall back
                            format!("creating tokio-epoll-uring fails with error: {e:#}")
                        }
                };
                    FeatureTestResult::Worse {
                        engine: IoEngineKind::StdFs,
                        remark,
                    }
                }
            })
        }
    })
    .join()
    .unwrap()
}

/// For use in benchmark binaries only.
///
/// Benchmarks which initialize `virtual_file` need to know what engine to use, but we also
/// don't want to silently fall back to slower I/O engines in a benchmark: this could waste
/// developer time trying to figure out why it's slow.
///
/// In practice, this method will either return IoEngineKind::TokioEpollUring, or panic.
pub fn io_engine_for_bench() -> IoEngineKind {
    #[cfg(not(target_os = "linux"))]
    {
        panic!("This benchmark does I/O and can only give a representative result on Linux");
    }
    #[cfg(target_os = "linux")]
    {
        match feature_test().unwrap() {
            FeatureTestResult::PlatformPreferred(engine) => engine,
            FeatureTestResult::Worse {
                engine: _engine,
                remark,
            } => {
                panic!("This benchmark does I/O can requires the preferred I/O engine: {remark}");
            }
        }
    }
}
