use pageserver_api::models::{AuxFilePolicy, TenantConfig, TenantConfigRequest};
use pageserver_api::shard::TenantShardId;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::util::tokio_thread_local_stats::AllThreadLocalStats;
use crate::util::{request_stats, tokio_thread_local_stats};

/// Ingest aux files into the pageserver.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,

    targets: Option<Vec<TenantTimelineId>>,
}

tokio_thread_local_stats::declare!(STATS: request_stats::Stats);

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    tokio_thread_local_stats::main!(STATS, move |thread_local_stats| {
        main_impl(args, thread_local_stats)
    })
}

async fn main_impl(
    args: Args,
    _all_thread_local_stats: AllThreadLocalStats<request_stats::Stats>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: Some(1),
            targets: args.targets.clone(),
        },
    )
    .await?;

    let timeline = timelines[0];
    let tenant_shard_id = TenantShardId::unsharded(timeline.tenant_id);
    let timeline_id = timeline.timeline_id;

    println!("operating on timeline {}", timeline);

    mgmt_api_client
        .tenant_config(&TenantConfigRequest {
            tenant_id: timeline.tenant_id,
            config: TenantConfig {
                switch_aux_file_policy: Some(AuxFilePolicy::V2),
                ..Default::default()
            },
        })
        .await?;

    for batch in 0..100 {
        let items = (0..100)
            .map(|id| {
                (
                    format!("pg_logical/mappings/{:03}.{:03}", batch, id),
                    format!("{:08}", id),
                )
            })
            .collect::<HashMap<_, _>>();
        let file_cnt = items.len();
        mgmt_api_client
            .ingest_aux_files(tenant_shard_id, timeline_id, items)
            .await?;
        println!("ingested {file_cnt} files");
    }

    let files = mgmt_api_client
        .list_aux_files(tenant_shard_id, timeline_id, Lsn(Lsn::MAX.0 - 1))
        .await?;

    println!("{} files found", files.len());

    anyhow::Ok(())
}
