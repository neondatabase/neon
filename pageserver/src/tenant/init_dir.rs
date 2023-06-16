use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Path, PathBuf},
};

use anyhow::Context;

use tracing::warn;
use utils::id::TimelineId;

use crate::is_uninit_mark;

fn timeline_id_from_path(path: &Path) -> anyhow::Result<TimelineId> {
    path.file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TimelineId>()
        .with_context(|| {
            format!(
                "Could not parse timeline id out of the path {}",
                path.display()
            )
        })
}

pub enum Decision {
    Load,
    RemoveTemporary(PathBuf),
    RemoveUninitMark(PathBuf),
}

#[derive(Default)]
pub struct TimelinesToLoad {
    entries: HashMap<TimelineId, Decision>,
}

impl TryFrom<&Path> for TimelinesToLoad {
    type Error = anyhow::Error;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let mut to_load = Self::default();

        for entry in std::fs::read_dir(value).context("read_dir")? {
            let entry = entry.context("read timeline dir entry")?;
            let entry_path = entry.path();
            if crate::is_temporary(&entry_path) {
                let timeline_id = timeline_id_from_path(&entry_path)?;
                to_load.record_temporary(timeline_id, entry_path)
            } else if is_uninit_mark(&entry_path) {
                let timeline_id = timeline_id_from_path(&entry_path)?;
                to_load.record_uninit(timeline_id, entry_path)?
            } else {
                match timeline_id_from_path(&entry_path) {
                    Ok(timeline_id) => to_load.record_load(timeline_id)?,
                    Err(_) => {
                        // A file or directory that doesn't look like a timeline ID
                        warn!(
                            "unexpected file or directory in timelines directory: {}",
                            entry_path.display()
                        );
                    }
                }
            }
        }

        Ok(to_load)
    }
}

impl TimelinesToLoad {
    fn record_temporary(&mut self, timeline_id: TimelineId, path: PathBuf) {
        self.entries
            .insert(timeline_id, Decision::RemoveTemporary(path));
    }

    fn record_uninit(&mut self, timeline_id: TimelineId, path: PathBuf) -> anyhow::Result<()> {
        use std::collections::hash_map::Entry::*;
        match self.entries.entry(timeline_id) {
            Occupied(mut o) => {
                let decision = o.get_mut();
                match decision {
                    Decision::Load => {
                        let _ = std::mem::replace(decision, Decision::RemoveUninitMark(path));
                    }
                    Decision::RemoveTemporary(_) => anyhow::bail!(
                        "there shouldnt be uninit marks for temporary timelines: {timeline_id}"
                    ), // FIXME is it actually true? Can there be both?
                    Decision::RemoveUninitMark(_) => {
                        anyhow::bail!(
                            "cannot have two uninit marks for the same timelie id: {timeline_id}"
                        )
                    }
                }
            }
            Vacant(v) => {
                v.insert(Decision::RemoveUninitMark(path));
            }
        }
        Ok(())
    }

    fn record_load(&mut self, timeline_id: TimelineId) -> anyhow::Result<()> {
        use std::collections::hash_map::Entry::*;
        match self.entries.entry(timeline_id) {
            Occupied(mut o) => {
                let decision = o.get_mut();
                match decision {
                    Decision::Load => {
                        anyhow::bail!("cant be duplicate timeline")
                    }
                    Decision::RemoveTemporary(_) => anyhow::bail!(
                        "there shouldnt be uninit marks for temporary timelines: {timeline_id}"
                    ), // FIXME is it actually true?
                    Decision::RemoveUninitMark(_) => {
                        // In case uninit mark exists it takes precendence
                    }
                }
            }
            Vacant(v) => {
                v.insert(Decision::Load);
            }
        }
        Ok(())
    }

    pub fn into_entries(self) -> HashMap<TimelineId, Decision> {
        self.entries
    }
}

// TODO unit tests
