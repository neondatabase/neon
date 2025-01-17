use std::str::FromStr;

use anyhow::{anyhow, Context};
use async_stream::{stream, try_stream};
use futures::StreamExt;
use remote_storage::{GenericRemoteStorage, ListingMode, ListingObject, RemotePath};
use tokio_stream::Stream;

use crate::{
    list_objects_with_retries, stream_objects_with_retries, RootTarget, S3Target,
    TenantShardTimelineId,
};
use pageserver_api::shard::TenantShardId;
use utils::id::{TenantId, TimelineId};

/// Given a remote storage and a target, output a stream of TenantIds discovered via listing prefixes
pub fn stream_tenants<'a>(
    remote_client: &'a GenericRemoteStorage,
    target: &'a RootTarget,
) -> impl Stream<Item = anyhow::Result<TenantShardId>> + 'a {
    stream_tenants_maybe_prefix(remote_client, target, None)
}
/// Given a remote storage and a target, output a stream of TenantIds discovered via listing prefixes
pub fn stream_tenants_maybe_prefix<'a>(
    remote_client: &'a GenericRemoteStorage,
    target: &'a RootTarget,
    tenant_id_prefix: Option<String>,
) -> impl Stream<Item = anyhow::Result<TenantShardId>> + 'a {
    try_stream! {
        let mut tenants_target = target.tenants_root();
        if let Some(tenant_id_prefix) = tenant_id_prefix {
            tenants_target.prefix_in_bucket += &tenant_id_prefix;
        }
        let mut tenants_stream =
            std::pin::pin!(stream_objects_with_retries(remote_client, ListingMode::WithDelimiter, &tenants_target));
        while let Some(chunk) = tenants_stream.next().await {
            let chunk = chunk?;
            let entry_ids = chunk.prefixes.iter()
                .map(|prefix| prefix.get_path().file_name().ok_or_else(|| anyhow!("no final component in path '{prefix}'")));
            for dir_name_res in entry_ids {
                let dir_name = dir_name_res?;
                let id = TenantShardId::from_str(dir_name)?;
                yield id;
            }
        }
    }
}

pub async fn stream_tenant_shards<'a>(
    remote_client: &'a GenericRemoteStorage,
    target: &'a RootTarget,
    tenant_id: TenantId,
) -> anyhow::Result<impl Stream<Item = Result<TenantShardId, anyhow::Error>> + 'a> {
    let shards_target = target.tenant_shards_prefix(&tenant_id);

    let strip_prefix = target.tenants_root().prefix_in_bucket;
    let prefix_str = &strip_prefix.strip_prefix("/").unwrap_or(&strip_prefix);

    tracing::info!("Listing shards in {}", shards_target.prefix_in_bucket);
    let listing =
        list_objects_with_retries(remote_client, ListingMode::WithDelimiter, &shards_target)
            .await?;

    let tenant_shard_ids = listing
        .prefixes
        .iter()
        .map(|prefix| prefix.get_path().as_str())
        .filter_map(|prefix| -> Option<&str> { prefix.strip_prefix(prefix_str) })
        .map(|entry_id_str| {
            let first_part = entry_id_str.split('/').next().unwrap();

            first_part
                .parse::<TenantShardId>()
                .with_context(|| format!("Incorrect tenant entry id str: {first_part}"))
        })
        .collect::<Vec<_>>();

    tracing::debug!("Yielding {} shards for {tenant_id}", tenant_shard_ids.len());
    Ok(stream! {
        for i in tenant_shard_ids {
            let id = i?;
            yield Ok(id);
        }
    })
}

/// Given a `TenantShardId`, output a stream of the timelines within that tenant, discovered
/// using a listing.
///
/// The listing is done before the stream is built, so that this
/// function can be used to generate concurrency on a stream using buffer_unordered.
pub async fn stream_tenant_timelines<'a>(
    remote_client: &'a GenericRemoteStorage,
    target: &'a RootTarget,
    tenant: TenantShardId,
) -> anyhow::Result<impl Stream<Item = Result<TenantShardTimelineId, anyhow::Error>> + 'a> {
    let mut timeline_ids: Vec<Result<TimelineId, anyhow::Error>> = Vec::new();
    let timelines_target = target.timelines_root(&tenant);

    let prefix_str = &timelines_target
        .prefix_in_bucket
        .strip_prefix("/")
        .unwrap_or(&timelines_target.prefix_in_bucket);

    let mut objects_stream = std::pin::pin!(stream_objects_with_retries(
        remote_client,
        ListingMode::WithDelimiter,
        &timelines_target
    ));
    loop {
        tracing::debug!("Listing in {tenant}");
        let fetch_response = match objects_stream.next().await {
            None => break,
            Some(Err(e)) => {
                timeline_ids.push(Err(e));
                break;
            }
            Some(Ok(r)) => r,
        };

        let new_entry_ids = fetch_response
            .prefixes
            .iter()
            .filter_map(|prefix| -> Option<&str> {
                prefix.get_path().as_str().strip_prefix(prefix_str)
            })
            .map(|entry_id_str| {
                let first_part = entry_id_str.split('/').next().unwrap();
                first_part
                    .parse::<TimelineId>()
                    .with_context(|| format!("Incorrect timeline entry id str: {entry_id_str}"))
            });

        for i in new_entry_ids {
            timeline_ids.push(i);
        }
    }

    tracing::debug!("Yielding {} timelines for {}", timeline_ids.len(), tenant);
    Ok(stream! {
        for i in timeline_ids {
            let id = i?;
            yield Ok(TenantShardTimelineId::new(tenant, id));
        }
    })
}

pub(crate) fn stream_listing<'a>(
    remote_client: &'a GenericRemoteStorage,
    target: &'a S3Target,
) -> impl Stream<Item = anyhow::Result<(RemotePath, Option<ListingObject>)>> + 'a {
    let listing_mode = if target.delimiter.is_empty() {
        ListingMode::NoDelimiter
    } else {
        ListingMode::WithDelimiter
    };
    try_stream! {
        let mut objects_stream = std::pin::pin!(stream_objects_with_retries(
            remote_client,
            listing_mode,
            target,
        ));
        while let Some(list) = objects_stream.next().await {
            let list = list?;
            if target.delimiter.is_empty() {
                for key in list.keys {
                    yield (key.key.clone(), Some(key));
                }
            } else {
                for key in list.prefixes {
                    yield (key, None);
                }
            }
        }
    }
}
