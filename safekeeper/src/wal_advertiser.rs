pub mod advmap {
    use std::sync::Arc;

    use utils::id::TenantId;

    use crate::timeline::Timeline;

    pub struct World {}
    pub struct SafekeeperTimelineHandle {}

    impl World {
        pub fn update_pageserver_attachments(
            &self,
            tenant_id: TenantId,
            update: safekeeper_api::models::TenantShardPageserverAttachmentChange,
        ) -> anyhow::Result<()> {
            todo!()
        }
        pub fn register_timeline(
            &self,
            tli: Arc<Timeline>,
        ) -> anyhow::Result<SafekeeperTimelineHandle> {
            todo!()
        }
    }
    impl SafekeeperTimelineHandle {
        pub fn ready_for_eviction(&self) -> bool {
            todo!()
        }
    }
    impl Default for World {
        fn default() -> Self {
            todo!()
        }
    }
}
