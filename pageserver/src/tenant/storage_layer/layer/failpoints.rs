//! failpoints for unit tests, implying `#[cfg(test)]`.
//!
//! These are not accessible over http.

use super::*;

impl LayerInner {
    /// Query if this failpoint is enabled, as in, arrive at a failpoint.
    ///
    /// Calls to this method need to be `#[cfg(test)]` guarded.
    pub(super) async fn failpoint(&self, kind: FailpointKind) -> Result<(), FailpointHit> {
        let fut = {
            let mut fps = self.failpoints.lock().unwrap();
            let fp = fps.iter_mut().find(|x| x.kind() == kind);

            let Some(fp) = fp else {
                return Ok(());
            };

            fp.hit()
        };

        fut.await
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

    fn hit(&mut self) -> impl std::future::Future<Output = Result<(), FailpointHit>> + 'static {
        use futures::future::FutureExt;

        // use boxed futures to avoid Either hurdles
        match self {
            Failpoint::AfterDeterminingLayerNeedsNoDownload => {
                let kind = self.kind();

                async move { Err(FailpointHit(kind)) }.boxed()
            }
            Failpoint::WaitBeforeStartingEvicting(b) => {
                let b = b.clone();

                async move {
                    tracing::trace!("waiting on a failpoint barrier");
                    b.wait().await;
                    tracing::trace!("done waiting on a failpoint barrier");
                    Ok(())
                }
                .boxed()
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
