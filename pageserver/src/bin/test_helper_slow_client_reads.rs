use std::{
    io::{stdin, stdout, Read, Write},
    time::Duration,
};

use clap::Parser;
use pageserver_api::models::{PagestreamRequest, PagestreamTestRequest};
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

#[derive(clap::Parser)]
struct Args {
    connstr: String,
    tenant_id: TenantId,
    timeline_id: TimelineId,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        connstr,
        tenant_id,
        timeline_id,
    } = Args::parse();
    let client = pageserver_client::page_service::Client::new(connstr).await?;
    let client = client.pagestream(tenant_id, timeline_id).await?;
    let (mut sender, _receiver) = client.split();

    eprintln!("filling the pipe");
    let mut msg = 0;
    loop {
        msg += 1;
        let fut = sender.send(pageserver_api::models::PagestreamFeMessage::Test(
            PagestreamTestRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(23),
                    not_modified_since: Lsn(23),
                },
                batch_key: 42,
                message: format!("message {}", msg),
            },
        ));
        let Ok(res) = tokio::time::timeout(Duration::from_secs(10), fut).await else {
            eprintln!("pipe seems full");
            break;
        };
        let _: () = res?;
    }

    let n = stdout().write(b"R")?;
    assert_eq!(n, 1);
    stdout().flush()?;

    eprintln!("waiting for signal to tell us to exit");

    let mut buf = [0u8; 1];
    stdin().read_exact(&mut buf)?;

    eprintln!("termination signal received, exiting");

    anyhow::Ok(())
}
