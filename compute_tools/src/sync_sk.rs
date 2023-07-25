// Utils for running sync_safekeepers
use anyhow::Result;
use tracing::info;
use utils::lsn::Lsn;

#[derive(Copy, Clone, Debug)]
pub enum TimelineStatusResponse {
    NotFound,
    Ok(TimelineStatusOkResponse),
}

#[derive(Copy, Clone, Debug)]
pub struct TimelineStatusOkResponse {
    flush_lsn: Lsn,
    commit_lsn: Lsn,
}

/// Get a safekeeper's metadata for our timeline
pub async fn ping_safekeeper(config: tokio_postgres::Config) -> Result<TimelineStatusResponse> {
    // TODO add retries

    // Connect
    info!("connecting to {:?}", config);
    let (client, conn) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Query
    info!("querying {:?}", config);
    let result = client.simple_query("TIMELINE_STATUS").await?;

    // Parse result
    info!("done with {:?}", config);
    if let postgres::SimpleQueryMessage::Row(row) = &result[0] {
        use std::str::FromStr;
        let response = TimelineStatusResponse::Ok(TimelineStatusOkResponse {
            flush_lsn: Lsn::from_str(row.get("flush_lsn").unwrap())?,
            commit_lsn: Lsn::from_str(row.get("commit_lsn").unwrap())?,
        });
        Ok(response)
    } else {
        // Timeline doesn't exist
        Ok(TimelineStatusResponse::NotFound)
    }
}

/// Given a quorum of responses, check if safekeepers are synced at some Lsn
pub fn check_if_synced(responses: &[TimelineStatusResponse; 2]) -> Option<Lsn> {
    info!(
        "checking sk responses {:?} {:?}",
        responses[0], responses[1]
    );
    match (responses[0], responses[1]) {
        (TimelineStatusResponse::Ok(r1), TimelineStatusResponse::Ok(r2)) => {
            // Check that quorum has identical commit and flush lsns
            if r1.commit_lsn == r1.flush_lsn
                && r1.commit_lsn == r2.commit_lsn
                && r1.flush_lsn == r2.flush_lsn
            {
                Some(r1.commit_lsn)
            } else {
                None
            }
        }
        _ => None,
    }
}
