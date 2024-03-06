//! failpoints for unit tests, implying `#[cfg(test)]`.
//!
//! These are not accessible over http.

use super::*;

impl LayerInner {
    /// Query if this failpoint is enabled, as in, arrive at a failpoint.
    ///
    /// Calls to this method need to be `#[cfg(test)]` guarded.
    pub(super) async fn failpoint(&self, kind: FailpointKind) -> Result<(), FailpointHit> {
        let fp = {
            let fps = self.failpoints.lock().unwrap();
            fps.iter().find(|x| x.kind() == kind).cloned()
        };
        if let Some(fp) = fp {
            fp.hit().await
        } else {
            Ok(())
        }
    }

    /// Enable a failpoint from a unit test.
    pub(super) fn enable_failpoint(&self, failpoint: Failpoint) {
        self.failpoints.lock().unwrap().push(failpoint);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailpointKind {
    /// Failpoint acts as an accurate cancelled by drop here; see the only site of use.
    AfterDeterminingLayerNeedsNoDownload,
    /// Failpoint for stalling eviction starting
    WaitBeforeStartingEvicting,
}

#[derive(Clone)]
pub(crate) enum Failpoint {
    /// Failpoint acts as an accurate cancelled by drop here; see the only site of use.
    AfterDeterminingLayerNeedsNoDownload,
    /// Failpoint for stalling eviction starting
    WaitBeforeStartingEvicting(utils::completion::Barrier),
}

impl Failpoint {
    fn kind(&self) -> FailpointKind {
        match self {
            Failpoint::AfterDeterminingLayerNeedsNoDownload => {
                FailpointKind::AfterDeterminingLayerNeedsNoDownload
            }
            Failpoint::WaitBeforeStartingEvicting(_) => FailpointKind::WaitBeforeStartingEvicting,
        }
    }

    async fn hit(&self) -> Result<(), FailpointHit> {
        match self {
            Failpoint::AfterDeterminingLayerNeedsNoDownload => Err(FailpointHit(self.kind())),
            Failpoint::WaitBeforeStartingEvicting(b) => {
                tracing::trace!("waiting on a failpoint barrier");
                b.clone().wait().await;
                tracing::trace!("done waiting on a failpoint barrier");
                Ok(())
            }
        }
    }
}

impl std::fmt::Display for FailpointKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug)]
pub(crate) struct FailpointHit(FailpointKind);

impl std::fmt::Display for FailpointHit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for FailpointHit {}

impl From<FailpointHit> for DownloadError {
    fn from(value: FailpointHit) -> Self {
        DownloadError::Failpoint(value.0)
    }
}
