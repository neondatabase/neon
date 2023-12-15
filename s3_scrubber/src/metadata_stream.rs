use anyhow::Context;
use async_stream::{stream, try_stream};
use aws_sdk_s3::{types::ObjectIdentifier, Client};
use tokio_stream::Stream;

use crate::{list_objects_with_retries, RootTarget, S3Target, TenantShardTimelineId};
use pageserver_api::shard::TenantShardId;
use utils::id::TimelineId;

/// Given an S3 bucket, output a stream of TenantIds discovered via ListObjectsv2
pub fn stream_tenants<'a>(
    s3_client: &'a Client,
    target: &'a RootTarget,
) -> impl Stream<Item = anyhow::Result<TenantShardId>> + 'a {
    try_stream! {
        let mut continuation_token = None;
        let tenants_target = target.tenants_root();
        loop {
            let fetch_response =
                list_objects_with_retries(s3_client, &tenants_target, continuation_token.clone()).await?;

            let new_entry_ids = fetch_response
                .common_prefixes()
                .iter()
                .filter_map(|prefix| prefix.prefix())
                .filter_map(|prefix| -> Option<&str> {
                    prefix
                        .strip_prefix(&tenants_target.prefix_in_bucket)?
                        .strip_suffix('/')
                }).map(|entry_id_str| {
                entry_id_str
                    .parse()
                    .with_context(|| format!("Incorrect entry id str: {entry_id_str}"))
            });

            for i in new_entry_ids {
                yield i?;
            }

            match fetch_response.next_continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }
    }
}

/// Given a TenantShardId, output a stream of the timelines within that tenant, discovered
/// using ListObjectsv2.  The listing is done before the stream is built, so that this
/// function can be used to generate concurrency on a stream using buffer_unordered.
pub async fn get_tenant_timelines(
    s3_client: &Client,
    target: &RootTarget,
    tenant: TenantShardId,
) -> Vec<anyhow::Result<TimelineId>> {
    let mut timeline_ids = Vec::new();
    let mut continuation_token = None;
    let timelines_target = target.timelines_root(&tenant);

    loop {
        tracing::trace!("Listing in {}", tenant);
        let fetch_response =
            list_objects_with_retries(s3_client, &timelines_target, continuation_token.clone())
                .await;
        let fetch_response = match fetch_response {
            Err(e) => {
                timeline_ids.push(Err(e));
                break;
            }
            Ok(r) => r,
        };

        let new_entry_ids = fetch_response
            .common_prefixes()
            .iter()
            .filter_map(|prefix| prefix.prefix())
            .filter_map(|prefix| -> Option<&str> {
                prefix
                    .strip_prefix(&timelines_target.prefix_in_bucket)?
                    .strip_suffix('/')
            })
            .map(|entry_id_str| {
                entry_id_str
                    .parse::<TimelineId>()
                    .with_context(|| format!("Incorrect entry id str: {entry_id_str}"))
            });

        for i in new_entry_ids {
            timeline_ids.push(i);
        }

        match fetch_response.next_continuation_token {
            Some(new_token) => continuation_token = Some(new_token),
            None => break,
        }
    }

    timeline_ids
}

pub async fn stream_tenant_timelines<'a>(
    client: &'a Client,
    target: &'a RootTarget,
    tenant: TenantShardId,
) -> anyhow::Result<impl Stream<Item = Result<TenantShardTimelineId, anyhow::Error>> + 'a> {
    let timelines = get_tenant_timelines(client, target, tenant).await;

    // FIXME: futures is not yet imported so have to keep doing it like this:
    Ok(stream! {
        for i in timelines {
            let id = i?;
            yield Ok(TenantShardTimelineId::new(tenant, id));
        }
    })
}

pub(crate) fn stream_listing<'a>(
    s3_client: &'a Client,
    target: &'a S3Target,
) -> impl Stream<Item = anyhow::Result<ObjectIdentifier>> + 'a {
    try_stream! {
        let mut continuation_token = None;
        loop {
            let fetch_response =
                list_objects_with_retries(s3_client, target, continuation_token.clone()).await?;

            if target.delimiter.is_empty() {
                for object_key in fetch_response.contents().iter().filter_map(|object| object.key())
                {
                    let object_id = ObjectIdentifier::builder().key(object_key).build()?;
                    yield object_id;
                }
            } else {
                for prefix in fetch_response.common_prefixes().iter().filter_map(|p| p.prefix()) {
                    let object_id = ObjectIdentifier::builder().key(prefix).build()?;
                    yield object_id;
                }
            }

            match fetch_response.next_continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }
    }
}
