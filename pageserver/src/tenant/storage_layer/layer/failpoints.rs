//! failpoints for unit tests, implying `#[cfg(test)]`.
//!
//! These are not accessible over http.

use super::*;

impl LayerInner {
    #[cfg(test)]
    pub(super) fn failpoint(&self, kind: FailpointKind) -> Result<(), FailpointHit> {
        let hit = self.failpoints.lock().unwrap().contains(kind);
        if hit {
            Err(FailpointHit(kind))
        } else {
            Ok(())
        }
    }

    pub(super) fn enable_failpoint(&self, kind: FailpointKind) {
        assert!(self.failpoints.lock().unwrap().insert(kind));
    }
}

#[derive(Debug, enumset::EnumSetType)]
#[cfg(test)]
pub(crate) enum FailpointKind {
    /// Failpoint acts as an accurate cancelled by drop here; see the only site of use.
    AfterDeterminingLayerNeedsNoDownload,
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
