use tracing::warn;
use utils::lsn::Lsn;

#[derive(Default)]
pub(crate) struct GlobalState {
    min_inflight_request_lsn: tokio::sync::watch::Sender<Option<Lsn>>,
}

impl GlobalState {
    pub fn update_min_inflight_request_lsn(&self, update: Lsn) {
        self.min_inflight_request_lsn.send_if_modified(|value| {
            let modified = *value != Some(update);
            if let Some(value) = *value && value > update {
                warn!(current=%value, new=%update, "min inflight request lsn moving backwards, this should not happen, bug in communicator");
            }
            *value = Some(update);
            modified
        });
    }
}
