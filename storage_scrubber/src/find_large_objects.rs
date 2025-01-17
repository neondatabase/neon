use std::pin::pin;

use futures::{StreamExt, TryStreamExt};
use pageserver::tenant::storage_layer::LayerName;
use remote_storage::ListingMode;
use serde::{Deserialize, Serialize};

use crate::{
    checks::parse_layer_object_name, init_remote, metadata_stream::stream_tenants,
    stream_objects_with_retries, BucketConfig, NodeKind,
};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum LargeObjectKind {
    DeltaLayer,
    ImageLayer,
    Other,
}

impl LargeObjectKind {
    fn from_key(key: &str) -> Self {
        let fname = key.split('/').last().unwrap();

        let Ok((layer_name, _generation)) = parse_layer_object_name(fname) else {
            return LargeObjectKind::Other;
        };

        match layer_name {
            LayerName::Image(_) => LargeObjectKind::ImageLayer,
            LayerName::Delta(_) => LargeObjectKind::DeltaLayer,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LargeObject {
    pub key: String,
    pub size: u64,
    kind: LargeObjectKind,
}

#[derive(Serialize, Deserialize)]
pub struct LargeObjectListing {
    pub objects: Vec<LargeObject>,
}

pub async fn find_large_objects(
    bucket_config: BucketConfig,
    min_size: u64,
    ignore_deltas: bool,
    concurrency: usize,
) -> anyhow::Result<LargeObjectListing> {
    let (remote_client, target) = init_remote(bucket_config.clone(), NodeKind::Pageserver).await?;
    let tenants = pin!(stream_tenants(&remote_client, &target));

    let objects_stream = tenants.map_ok(|tenant_shard_id| {
        let mut tenant_root = target.tenant_root(&tenant_shard_id);
        let remote_client = remote_client.clone();
        async move {
            let mut objects = Vec::new();
            let mut total_objects_ctr = 0u64;
            // We want the objects and not just common prefixes
            tenant_root.delimiter.clear();
            let mut objects_stream = pin!(stream_objects_with_retries(
                &remote_client,
                ListingMode::NoDelimiter,
                &tenant_root
            ));
            while let Some(listing) = objects_stream.next().await {
                let listing = listing?;
                for obj in listing.keys.iter().filter(|obj| min_size <= obj.size) {
                    let key = obj.key.to_string();
                    let kind = LargeObjectKind::from_key(&key);
                    if ignore_deltas && kind == LargeObjectKind::DeltaLayer {
                        continue;
                    }
                    objects.push(LargeObject {
                        key,
                        size: obj.size,
                        kind,
                    })
                }
                total_objects_ctr += listing.keys.len() as u64;
            }

            Ok((tenant_shard_id, objects, total_objects_ctr))
        }
    });
    let mut objects_stream = std::pin::pin!(objects_stream.try_buffer_unordered(concurrency));

    let mut objects = Vec::new();

    let mut tenant_ctr = 0u64;
    let mut object_ctr = 0u64;
    while let Some(res) = objects_stream.next().await {
        let (tenant_shard_id, objects_slice, total_objects_ctr) = res?;
        objects.extend_from_slice(&objects_slice);

        object_ctr += total_objects_ctr;
        tenant_ctr += 1;
        if tenant_ctr % 100 == 0 {
            tracing::info!(
                "Scanned {tenant_ctr} shards. objects={object_ctr}, found={}, current={tenant_shard_id}.",
                objects.len()
            );
        }
    }

    let desc_str = target.desc_str();
    tracing::info!(
        "Scan of {desc_str} finished. Scanned {tenant_ctr} shards. objects={object_ctr}, found={}.",
        objects.len()
    );
    Ok(LargeObjectListing { objects })
}
