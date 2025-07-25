use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use tonic::transport::Endpoint;
use tracing::info;

use pageserver_page_api::{GetPageClass, GetPageRequest, GetPageStatusCode, ReadLsn, RelTag};
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;
use utils::shard::ShardIndex;

/// Starts a large number of idle gRPC GetPage streams.
#[derive(clap::Parser)]
pub(crate) struct Args {
    /// The Pageserver to connect to. Must use grpc://.
    #[clap(long, default_value = "grpc://localhost:51051")]
    server: String,
    /// The Pageserver HTTP API.
    #[clap(long, default_value = "http://localhost:9898")]
    http_server: String,
    /// The number of streams to open.
    #[clap(long, default_value = "100000")]
    count: usize,
    /// Number of streams per connection.
    #[clap(long, default_value = "100")]
    per_connection: usize,
    /// Send a single GetPage request on each stream.
    #[clap(long, default_value_t = false)]
    send_request: bool,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(main_impl(args))
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    // Discover a tenant and timeline to use.
    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        reqwest::Client::new(),
        args.http_server.clone(),
        None,
    ));
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: Some(1),
            targets: None,
        },
    )
    .await?;
    let ttid = timelines
        .first()
        .ok_or_else(|| anyhow!("no timelines found"))?;

    // Set up the initial client.
    let endpoint = Endpoint::from_shared(args.server.clone())?;

    let connect = async || {
        pageserver_page_api::Client::new(
            endpoint.connect().await?,
            ttid.tenant_id,
            ttid.timeline_id,
            ShardIndex::unsharded(),
            None,
            None,
        )
    };

    let mut client = connect().await?;
    let mut streams = Vec::with_capacity(args.count);

    // Create streams.
    for i in 0..args.count {
        if i.is_multiple_of(100) {
            info!("opened {}/{} streams", i, args.count);
        }
        if i.is_multiple_of(args.per_connection) && i > 0 {
            client = connect().await?;
        }

        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
        let req_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(req_rx);
        let mut resp_stream = client.get_pages(req_stream).await?;

        // Send request if specified.
        if args.send_request {
            req_tx.send(GetPageRequest {
                request_id: 1.into(),
                request_class: GetPageClass::Normal,
                read_lsn: ReadLsn {
                    request_lsn: Lsn::MAX,
                    not_modified_since_lsn: Some(Lsn(1)),
                },
                rel: RelTag {
                    spcnode: 1664, // pg_global
                    dbnode: 0,     // shared database
                    relnode: 1262, // pg_authid
                    forknum: 0,    // init
                },
                block_numbers: vec![0],
            })?;
            let resp = resp_stream
                .next()
                .await
                .transpose()?
                .ok_or_else(|| anyhow!("no response"))?;
            if resp.status_code != GetPageStatusCode::Ok {
                return Err(anyhow!("{} response", resp.status_code));
            }
        }

        // Hold onto streams to avoid closing them.
        streams.push((req_tx, resp_stream));
    }

    info!("opened {} streams, sleeping", args.count);

    // Block forever, to hold the idle streams open for inspection.
    futures::future::pending::<()>().await;

    Ok(())
}
