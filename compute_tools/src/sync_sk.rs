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

/// Get a safekeeper's metadata for our timeline. The id is only used for logging
pub async fn ping_safekeeper(
    id: String,
    config: tokio_postgres::Config,
) -> Result<TimelineStatusResponse> {
    // TODO add retries

    // Connect
    info!("connecting to {}", id);
    let (client, conn) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Query
    info!("querying {}", id);
    let result = client.simple_query("TIMELINE_STATUS").await?;

    // Parse result
    info!("done with {}", id);
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
pub fn check_if_synced(responses: Vec<TimelineStatusResponse>) -> Option<Lsn> {
    // Check if all responses are ok
    let ok_responses: Vec<TimelineStatusOkResponse> = responses
        .iter()
        .filter_map(|r| match r {
            TimelineStatusResponse::Ok(ok_response) => Some(ok_response),
            _ => None,
        })
        .cloned()
        .collect();
    if ok_responses.len() < responses.len() {
        info!(
            "not synced. Only {} out of {} know about this timeline",
            ok_responses.len(),
            responses.len()
        );
        return None;
    }

    // Get the min and the max of everything
    let commit: Vec<Lsn> = ok_responses.iter().map(|r| r.commit_lsn).collect();
    let flush: Vec<Lsn> = ok_responses.iter().map(|r| r.flush_lsn).collect();
    let commit_max = commit.iter().max().unwrap();
    let commit_min = commit.iter().min().unwrap();
    let flush_max = flush.iter().max().unwrap();
    let flush_min = flush.iter().min().unwrap();

    // Check that all values are equal
    if commit_min != commit_max {
        info!("not synced. {:?} {:?}", commit_min, commit_max);
        return None;
    }
    if flush_min != flush_max {
        info!("not synced. {:?} {:?}", flush_min, flush_max);
        return None;
    }

    // Check that commit == flush
    if commit_max != flush_max {
        info!("not synced. {:?} {:?}", commit_max, flush_max);
        return None;
    }

    Some(*commit_max)
}
