use anyhow::Result;
use serde::{Deserialize, Serialize};

use zenith_utils::lsn::Lsn;

use crate::{repository::Repository, ZTimelineId};

#[derive(Serialize, Deserialize)]
pub struct BranchInfo {
    pub name: String,
    pub timeline_id: String,
    pub latest_valid_lsn: Option<Lsn>,
}

pub(crate) fn get_branches(repository: &dyn Repository) -> Result<Vec<BranchInfo>> {
    // adapted from CLI code
    let branches_dir = std::path::Path::new("refs").join("branches");
    std::fs::read_dir(&branches_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            let name = dir_entry.file_name().to_str().unwrap().to_string();
            let timeline_id = std::fs::read_to_string(dir_entry.path())?.parse::<ZTimelineId>()?;

            let latest_valid_lsn = repository
                .get_timeline(timeline_id)
                .map(|timeline| timeline.get_last_valid_lsn())
                .ok();

            Ok(BranchInfo {
                name,
                timeline_id: timeline_id.to_string(),
                latest_valid_lsn,
            })
        })
        .collect()
}
