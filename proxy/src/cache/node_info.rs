use crate::cache::common::Cache;
use crate::cache::{Cached, ControlPlaneResult, CplaneExpiry};
use crate::config::CacheOptions;
use crate::control_plane::NodeInfo;
use crate::types::EndpointCacheKey;

pub(crate) struct NodeInfoCache(moka::sync::Cache<EndpointCacheKey, ControlPlaneResult<NodeInfo>>);
pub(crate) type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;

impl Cache for NodeInfoCache {
    type Key = EndpointCacheKey;
    type Value = ControlPlaneResult<NodeInfo>;

    fn invalidate(&self, info: &EndpointCacheKey) {
        self.0.invalidate(info);
    }
}

impl NodeInfoCache {
    pub fn new(config: CacheOptions) -> Self {
        let builder = moka::sync::Cache::builder()
            .name("node_info_cache")
            .expire_after(CplaneExpiry::default());
        let builder = config.moka(builder);
        Self(builder.build())
    }

    pub fn insert(&self, key: EndpointCacheKey, value: ControlPlaneResult<NodeInfo>) {
        self.0.insert(key, value);
    }

    pub fn get(&'static self, key: &EndpointCacheKey) -> Option<ControlPlaneResult<NodeInfo>> {
        self.0.get(key)
    }

    pub fn get_entry(
        &'static self,
        key: &EndpointCacheKey,
    ) -> Option<ControlPlaneResult<CachedNodeInfo>> {
        self.get(key).map(|res| {
            res.map(|value| Cached {
                token: Some((self, key.clone())),
                value,
            })
        })
    }
}
