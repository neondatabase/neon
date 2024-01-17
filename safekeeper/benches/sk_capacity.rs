use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use safekeeper_api::models::TimelineCreateRequest;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;

const ABOUT: &str = r#"
Creates many random timelines on the safekeeper.

For example,
cargo build -r -p safekeeper && target/release/safekeeper
cargo bench --bench sk_capacity -- -n 1000 --http-addr=http://127.0.0.1:7676
"#;

#[derive(Parser, Debug)]
#[clap(author, version, about = ABOUT)]
struct Args {
    /// Number of timelines to create
    #[clap(short = 'n', long, value_parser, default_value_t = 1)]
    num_timelines: u64,
    /// HTTP safekeeper address
    #[clap(long)]
    http_addr: String,
    // Fake value to satisfy `cargo bench` passing it.
    #[clap(long)]
    bench: bool,
}

async fn create_timeline(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    let ttid = TenantTimelineId::generate();

    let request = TimelineCreateRequest {
        tenant_id: ttid.tenant_id,
        timeline_id: ttid.timeline_id,
        peer_ids: None,
        pg_version: 160000,
        system_id: None,
        wal_seg_size: None,
        commit_lsn: Lsn(21623024),
        local_start_lsn: None,
    };

    // Send request to /v1/tenant/timeline
    let url = format!("{}/v1/tenant/timeline", args.http_addr);
    let res = client.post(url).json(&request).send().await?;

    println!("Response: {:?}", res.status());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    for i in 0..args.num_timelines {
        create_timeline(&args).await?;
    }
    Ok(())
}
