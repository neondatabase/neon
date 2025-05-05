use safekeeper_api::models::TenantShardPageserverLocation;
use utils::shard::TenantShardId;
use walproposer::bindings::Generation;

pub(crate) struct Tenant {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    data: HashMap<TenantShardPageserverLocation, PerLocationState>,
}

struct PerLocationState {
    remote_consistent_lsn: Lsn,
    last_update_at: Instant,
    dirty: bool,
}

impl Tenant {
    pub fn load(conf: Arc<SafeKeeperConf>, tenant_id: TenantId) -> anyhow::Result<Arc<Tenant>> {
        todo!()
    }

    pub fn get_locations(&self) -> anyhow::Result<Vec<TenantShardPageserverLocation>> {
        todo!()
    }
    pub fn update_locations(
        &self,
        locations: Vec<TenantShardPageserverLocation>,
    ) -> anyhow::Result<()> {
        todo!()
    }
    pub fn update_remote_consistent_lsn(
        &self,
        tenant_shard_id: TenantShardId,
        generation: Generation,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
