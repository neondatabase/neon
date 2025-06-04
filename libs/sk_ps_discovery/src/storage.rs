use std::collections::HashMap;

use utils::id::TenantTimelineId;

use crate::TimelineAttachmentId;

pub trait Storage {
    fn get_timeline(&self, ttid: TenantTimelineId) -> Timeline;
    fn store_timeline(&self, ttid: TenantTimelineId, timeline: Timeline);
}

pub struct Timeline {
    pub remote_consistent_lsns: HashMap<TimelineAttachmentId, Lsn>,
}
