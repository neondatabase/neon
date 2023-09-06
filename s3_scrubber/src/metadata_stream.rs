use anyhow::Context;
use async_stream::{stream, try_stream};
use aws_sdk_s3::Client;
use tokio_stream::Stream;

use crate::{list_objects_with_retries, RootTarget, TenantId};
use utils::id::{TenantTimelineId, TimelineId};

/// Given an S3 bucket, output a stream of TenantIds discovered via ListObjectsv2
pub fn stream_tenants<'a>(
    s3_client: &'a Client,
    target: &'a RootTarget,
) -> impl Stream<Item = anyhow::Result<TenantId>> + 'a {
    try_stream! {
        let mut continuation_token = None;
        loop {
            let tenants_target = target.tenants_root();
            let fetch_response =
                list_objects_with_retries(s3_client, tenants_target, continuation_token.clone()).await?;

            let new_entry_ids = fetch_response
                .common_prefixes()
                .unwrap_or_default()
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

/// Given a TenantId, output a stream of the timelines within that tenant, discovered
/// using ListObjectsv2.  The listing is done before the stream is built, so that this
/// function can be used to generate concurrency on a stream using buffer_unordered.
pub async fn stream_tenant_timelines<'a>(
    s3_client: &'a Client,
    target: &'a RootTarget,
    tenant: TenantId,
) -> anyhow::Result<impl Stream<Item = Result<TenantTimelineId, anyhow::Error>> + 'a> {
    let mut timeline_ids: Vec<Result<TimelineId, anyhow::Error>> = Vec::new();
    let mut continuation_token = None;
    let timelines_target = target.timelines_root(&tenant);

    loop {
        tracing::info!("Listing in {}", tenant);
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
            .unwrap_or_default()
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

    tracing::info!("Yielding for {}", tenant);
    Ok(stream! {
        for i in timeline_ids {
            let id = i?;
            yield Ok(TenantTimelineId::new(tenant, id));
        }
    })
}
