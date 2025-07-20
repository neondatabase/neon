use itertools::Itertools;
use utils::shard::{ShardCount, ShardNumber, TenantShardId};

use crate::compute::ParsedSpec;

pub struct ConnectInfo {
    pub tenant_shard_id: TenantShardId,
    pub connstring: String,
    pub auth: Option<String>,
}

pub fn pageserver_connstrings_for_connect(pspec: &ParsedSpec) -> Vec<ConnectInfo> {
    let connstrings = &pspec.pageserver_connstr;
    let auth = pspec.storage_auth_token.clone();

    let connstrings = connstrings.split(',').collect_vec();
    let shard_count = connstrings.len();

    let mut infos = Vec::with_capacity(connstrings.len());
    for (shard_number, connstring) in connstrings.iter().enumerate() {
        let tenant_shard_id = match shard_count {
            0 | 1 => TenantShardId::unsharded(pspec.tenant_id),
            shard_count => TenantShardId {
                tenant_id: pspec.tenant_id,
                shard_number: ShardNumber(shard_number as u8),
                shard_count: ShardCount::new(shard_count as u8),
            },
        };

        infos.push(ConnectInfo {
            tenant_shard_id,
            connstring: connstring.to_string(),
            auth: auth.clone(),
        });
    }

    infos
}
