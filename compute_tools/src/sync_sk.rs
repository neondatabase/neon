use anyhow::Result;

/// Get a safekeeper's metadata for our timeline
pub async fn ping_safekeeper(
    config: tokio_postgres::Config,
) -> Result<Option<(String, String, String)>> {
    // TODO add retries

    // Connect
    let (client, conn) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Query
    let result = client.simple_query("TIMELINE_STATUS").await?;

    // Parse result
    if let postgres::SimpleQueryMessage::Row(row) = &result[0] {
        let flush_lsn = row.get("flush_lsn").unwrap().to_string();
        let commit_lsn = row.get("commit_lsn").unwrap().to_string();
        let peer_horizon_lsn = row.get("peer_horizon_lsn").unwrap().to_string();
        Ok(Some((flush_lsn, commit_lsn, peer_horizon_lsn)))
    } else {
        // Timeline doesn't exist
        return Ok(None);
    }
}
