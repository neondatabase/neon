#[derive(Debug)]
pub enum DownloadError {
    /// Validation or other error happened due to user input.
    BadInput(anyhow::Error),
    /// The file was not found in the remote storage.
    NotFound,
    /// A cancellation token aborted the download, typically during
    /// tenant detach or process shutdown.
    Cancelled,
    /// A timeout happened while executing the request. Possible reasons:
    /// - stuck tcp connection
    ///
    /// Concurrency control is not timed within timeout.
    Timeout,
    /// The file was found in the remote storage, but the download failed.
    Other(anyhow::Error),
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::BadInput(e) => {
                write!(f, "Failed to download a remote file due to user input: {e}")
            }
            DownloadError::NotFound => write!(f, "No file found for the remote object id given"),
            DownloadError::Cancelled => write!(f, "Cancelled, shutting down"),
            DownloadError::Timeout => write!(f, "timeout"),
            DownloadError::Other(e) => write!(f, "Failed to download a remote file: {e:?}"),
        }
    }
}

impl std::error::Error for DownloadError {}

impl DownloadError {
    /// Returns true if the error should not be retried with backoff
    pub fn is_permanent(&self) -> bool {
        use DownloadError::*;
        match self {
            BadInput(_) => true,
            NotFound => true,
            Cancelled => true,
            Timeout => false,
            Other(_) => false,
        }
    }
}

#[derive(Debug)]
pub enum TimeTravelError {
    /// Validation or other error happened due to user input.
    BadInput(anyhow::Error),
    /// The used remote storage does not have time travel recovery implemented
    Unimplemented,
    /// The number of versions/deletion markers is above our limit.
    TooManyVersions,
    /// A cancellation token aborted the process, typically during
    /// request closure or process shutdown.
    Cancelled,
    /// Other errors
    Other(anyhow::Error),
}

impl std::fmt::Display for TimeTravelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeTravelError::BadInput(e) => {
                write!(
                    f,
                    "Failed to time travel recover a prefix due to user input: {e}"
                )
            }
            TimeTravelError::Unimplemented => write!(
                f,
                "time travel recovery is not implemented for the current storage backend"
            ),
            TimeTravelError::Cancelled => write!(f, "Cancelled, shutting down"),
            TimeTravelError::TooManyVersions => {
                write!(f, "Number of versions/delete markers above limit")
            }
            TimeTravelError::Other(e) => write!(f, "Failed to time travel recover a prefix: {e:?}"),
        }
    }
}

impl std::error::Error for TimeTravelError {}

/// Root cause for cancellations however this type does not implement `std::error::Error` so it
/// cannot be put as the root cause of `anyhow::Error`. It should never need to be exposed out of
/// this crate.
#[derive(Debug)]
pub(crate) struct Cancelled;

impl From<Cancelled> for anyhow::Error {
    fn from(_: Cancelled) -> Self {
        anyhow::Error::new(TimeoutOrCancel::Cancel)
    }
}

impl From<Cancelled> for TimeTravelError {
    fn from(_: Cancelled) -> Self {
        TimeTravelError::Cancelled
    }
}

impl From<Cancelled> for TimeoutOrCancel {
    fn from(_: Cancelled) -> Self {
        TimeoutOrCancel::Cancel
    }
}

impl From<Cancelled> for DownloadError {
    fn from(_: Cancelled) -> Self {
        DownloadError::Cancelled
    }
}

/// This type is used at as the root cause for timeouts and cancellations with anyhow returning
/// RemoteStorage methods.
#[derive(Debug)]
pub enum TimeoutOrCancel {
    Timeout,
    Cancel,
}

impl std::fmt::Display for TimeoutOrCancel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TimeoutOrCancel::*;
        match self {
            Timeout => write!(f, "timeout"),
            Cancel => write!(f, "cancel"),
        }
    }
}

impl std::error::Error for TimeoutOrCancel {}

impl TimeoutOrCancel {
    pub fn caused(error: &anyhow::Error) -> Option<&Self> {
        error.root_cause().downcast_ref()
    }

    /// Returns true if the error was caused by [`TimeoutOrCancel::Cancel`].
    pub fn caused_by_cancel(error: &anyhow::Error) -> bool {
        Self::caused(error).is_some_and(Self::is_cancel)
    }

    pub fn is_cancel(&self) -> bool {
        matches!(self, TimeoutOrCancel::Cancel)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, TimeoutOrCancel::Timeout)
    }
}

// Sadly the only way `tokio::io::copy_buf` helpers work is that if the error type is
// `std::io::Error`.
impl From<TimeoutOrCancel> for std::io::Error {
    fn from(value: TimeoutOrCancel) -> Self {
        let e = DownloadError::from(value);
        std::io::Error::other(e)
    }
}

impl From<TimeoutOrCancel> for DownloadError {
    fn from(value: TimeoutOrCancel) -> Self {
        use TimeoutOrCancel::*;

        match value {
            Timeout => DownloadError::Timeout,
            Cancel => DownloadError::Cancelled,
        }
    }
}
