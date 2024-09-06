use std::fs;

pub enum Metadata {
    StdFs(fs::Metadata),
    #[cfg(target_os = "linux")]
    TokioEpollUring(Box<tokio_epoll_uring::ops::statx::statx>),
}

#[cfg(target_os = "linux")]
impl From<Box<tokio_epoll_uring::ops::statx::statx>> for Metadata {
    fn from(value: Box<tokio_epoll_uring::ops::statx::statx>) -> Self {
        Metadata::TokioEpollUring(value)
    }
}

impl From<std::fs::Metadata> for Metadata {
    fn from(value: std::fs::Metadata) -> Self {
        Metadata::StdFs(value)
    }
}

impl Metadata {
    pub fn len(&self) -> u64 {
        match self {
            Metadata::StdFs(metadata) => metadata.len(),
            #[cfg(target_os = "linux")]
            Metadata::TokioEpollUring(statx) => statx.stx_size,
        }
    }
}
