use std::sync::Arc;

use humantime::Duration;
use tokio::task::JoinSet;
use utils::id::TenantTimelineId;

#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "localhost:64000")]
    page_service_host_port: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(
        long,
        help = "if specified, poll mgmt api to check whether init logical size calculation has completed"
    )]
    poll_for_completion: Option<Duration>,
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
    targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let main_task = rt.spawn(main_impl(args));
    rt.block_on(main_task).unwrap()
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: args.limit_to_first_n_targets,
            targets: args.targets.clone(),
        },
    )
    .await?;

    // kick it off

    let mut js = JoinSet::new();
    for tl in timelines {
        let mgmt_api_client = Arc::clone(&mgmt_api_client);
        js.spawn(async move {
            // TODO: API to explicitly trigger initial logical size computation.
            // Should probably also avoid making it a side effect of timeline details to trigger initial logical size calculation.
            // => https://github.com/neondatabase/neon/issues/6168
            let info = mgmt_api_client
                .timeline_info(tl.tenant_id, tl.timeline_id)
                .await
                .unwrap();

            if let Some(period) = args.poll_for_completion {
                let mut ticker = tokio::time::interval(period.into());
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut info = info;
                while !info.current_logical_size_is_accurate {
                    ticker.tick().await;
                    info = mgmt_api_client
                        .timeline_info(tl.tenant_id, tl.timeline_id)
                        .await
                        .unwrap();
                }
            }
        });
    }
    while let Some(res) = js.join_next().await {
        let _: () = res.unwrap();
    }
    Ok(())
}
