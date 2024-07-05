use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use futures::{StreamExt, TryStreamExt};
use pageserver::tenant::storage_layer::LayerName;
use serde::{Deserialize, Serialize};

use crate::{
    checks::parse_layer_object_name, init_remote, list_objects_with_retries,
    metadata_stream::stream_tenants, BucketConfig, NodeKind,
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
    let (s3_client, target) = init_remote(bucket_config.clone(), NodeKind::Pageserver)?;
    let tenants = std::pin::pin!(stream_tenants(&s3_client, &target));

    let object_ctr = Arc::new(AtomicU64::new(0));

    let objects_stream = tenants.map_ok(|tenant_shard_id| {
        let mut tenant_root = target.tenant_root(&tenant_shard_id);
        let object_ctr = object_ctr.clone();
        let s3_client = s3_client.clone();
        async move {
            let mut objects = Vec::new();
            // We want the objects and not just common prefixes
            tenant_root.delimiter.clear();
            let mut continuation_token = None;
            loop {
                let fetch_response =
                    list_objects_with_retries(&s3_client, &tenant_root, continuation_token.clone())
                        .await?;
                for obj in fetch_response.contents().iter().filter(|o| {
                    if let Some(obj_size) = o.size {
                        min_size as i64 <= obj_size
                    } else {
                        false
                    }
                }) {
                    let key = obj.key().expect("couldn't get key").to_owned();
                    let kind = LargeObjectKind::from_key(&key);
                    if ignore_deltas && kind == LargeObjectKind::DeltaLayer {
                        continue;
                    }
                    objects.push(LargeObject {
                        key,
                        size: obj.size.unwrap() as u64,
                        kind,
                    })
                }
                object_ctr.fetch_add(fetch_response.contents().len() as u64, Ordering::Relaxed);
                match fetch_response.next_continuation_token {
                    Some(new_token) => continuation_token = Some(new_token),
                    None => break,
                }
            }

            Ok((tenant_shard_id, objects))
        }
    });
    let mut objects_stream = std::pin::pin!(objects_stream.try_buffer_unordered(concurrency));

    let mut objects = Vec::new();

    let tenant_ctr = 0u64;
    while let Some(res) = objects_stream.next().await {
        let (tenant_shard_id, objects_slice) = res?;
        objects.extend_from_slice(&objects_slice);

        if tenant_ctr % 100 == 0 {
            let objects_count = object_ctr.load(Ordering::Relaxed);

            tracing::info!(
                "Scanned {tenant_ctr} shards. objects={objects_count}, found={}, current={tenant_shard_id}.",
                objects.len()
            );
        }
    }

    let bucket_name = target.bucket_name();
    let objects_count = object_ctr.load(Ordering::Relaxed);
    tracing::info!(
        "Scan of {bucket_name} finished. Scanned {tenant_ctr} shards. objects={objects_count}, found={}.",
        objects.len()
    );
    Ok(LargeObjectListing { objects })
}
