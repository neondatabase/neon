/// Reasons for downloads or listings to fail.
#[derive(Debug)]
pub enum DownloadError {
    /// Validation or other error happened due to user input.
    BadInput(anyhow::Error),
    /// The file was not found in the remote storage.
    NotFound,
    /// The caller provided an ETag, and the file was not modified.
    Unmodified,
    /// A cancellation token aborted the download, typically during
    /// tenant detach or process shutdown.
    Cancelled,
    /// A timeout happened while executing the request. Possible reasons:
    /// - stuck tcp connection
    ///
    /// Concurrency control is not timed within timeout.
    Timeout,
    /// Some integrity/consistency check failed during download. This is used during
    /// timeline loads to cancel the load of a tenant if some timeline detects fatal corruption.
    Fatal(String),
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
            DownloadError::Unmodified => write!(f, "File was not modified"),
            DownloadError::Cancelled => write!(f, "Cancelled, shutting down"),
            DownloadError::Timeout => write!(f, "timeout"),
            DownloadError::Fatal(why) => write!(f, "Fatal read error: {why}"),
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
            BadInput(_) | NotFound | Unmodified | Fatal(_) | Cancelled => true,
            Timeout | Other(_) => false,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, DownloadError::Cancelled)
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(value: std::io::Error) -> Self {
        let needs_unwrap = value.kind() == std::io::ErrorKind::Other
            && value
                .get_ref()
                .and_then(|x| x.downcast_ref::<DownloadError>())
                .is_some();

        if needs_unwrap {
            *value
                .into_inner()
                .expect("just checked")
                .downcast::<DownloadError>()
                .expect("just checked")
        } else {
            DownloadError::Other(value.into())
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

/// Plain cancelled error.
///
/// By design this type does not not implement `std::error::Error` so it cannot be put as the root
/// cause of `std::io::Error` or `anyhow::Error`. It should never need to be exposed out of this
/// crate.
///
/// It exists to implement permit acquiring in `{Download,TimeTravel}Error` and `anyhow::Error` returning
/// operations and ensuring that those get converted to proper versions with just `?`.
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

/// This type is used at as the root cause for timeouts and cancellations with `anyhow::Error` returning
/// RemoteStorage methods.
///
/// For use with `utils::backoff::retry` and `anyhow::Error` returning operations there is
/// `TimeoutOrCancel::caused_by_cancel` method to query "proper form" errors.
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
    /// Returns true if the error was caused by [`TimeoutOrCancel::Cancel`].
    pub fn caused_by_cancel(error: &anyhow::Error) -> bool {
        error
            .root_cause()
            .downcast_ref::<Self>()
            .is_some_and(Self::is_cancel)
    }

    pub fn is_cancel(&self) -> bool {
        matches!(self, TimeoutOrCancel::Cancel)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, TimeoutOrCancel::Timeout)
    }
}

/// This conversion is used when [`crate::support::DownloadStream`] notices a cancellation or
/// timeout to wrap it in an `std::io::Error`.
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
