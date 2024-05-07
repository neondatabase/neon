use utils::id::TimelineId;

#[derive(Default, serde::Serialize)]
pub struct AncestorDetached {
    pub reparented_timelines: Vec<TimelineId>,
}
