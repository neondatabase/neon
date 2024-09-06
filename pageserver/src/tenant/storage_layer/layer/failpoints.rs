//! failpoints for unit tests, implying `#[cfg(test)]`.
//!
//! These are not accessible over http.

use super::*;

impl Layer {
    /// Enable a failpoint from a unit test.
    pub(super) fn enable_failpoint(&self, failpoint: Failpoint) {
        self.0.failpoints.lock().unwrap().push(failpoint);
    }
}

impl LayerInner {
    /// Query if this failpoint is enabled, as in, arrive at a failpoint.
    ///
    /// Calls to this method need to be `#[cfg(test)]` guarded.
    pub(super) async fn failpoint(&self, kind: FailpointKind) -> Result<(), FailpointHit> {
        let fut = {
            let mut fps = self.failpoints.lock().unwrap();
            // find the *last* failpoint for cases in which we need to use multiple for the same
            // thing (two blocked evictions)
            let fp = fps.iter_mut().rfind(|x| x.kind() == kind);

            let Some(fp) = fp else {
                return Ok(());
            };

            fp.hit()
        };

        fut.await
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailpointKind {
    /// Failpoint acts as an accurate cancelled by drop here; see the only site of use.
    AfterDeterminingLayerNeedsNoDownload,
    /// Failpoint for stalling eviction starting
    WaitBeforeStartingEvicting,
    /// Failpoint hit in the spawned task
    WaitBeforeDownloading,
}

pub(crate) enum Failpoint {
    AfterDeterminingLayerNeedsNoDownload,
    WaitBeforeStartingEvicting(
        Option<utils::completion::Completion>,
        utils::completion::Barrier,
    ),
    WaitBeforeDownloading(
        Option<utils::completion::Completion>,
        utils::completion::Barrier,
    ),
}

impl Failpoint {
    fn kind(&self) -> FailpointKind {
        match self {
            Failpoint::AfterDeterminingLayerNeedsNoDownload => {
                FailpointKind::AfterDeterminingLayerNeedsNoDownload
            }
            Failpoint::WaitBeforeStartingEvicting(..) => FailpointKind::WaitBeforeStartingEvicting,
            Failpoint::WaitBeforeDownloading(..) => FailpointKind::WaitBeforeDownloading,
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
            Failpoint::WaitBeforeStartingEvicting(arrival, b)
            | Failpoint::WaitBeforeDownloading(arrival, b) => {
                // first one signals arrival
                drop(arrival.take());

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
