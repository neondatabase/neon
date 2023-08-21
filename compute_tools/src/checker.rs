use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio_postgres::NoTls;
use tracing::{error, instrument};

use crate::compute::ComputeNode;

/// Update timestamp in a row in a special service table to check
/// that we can actually write some data in this particular timeline.
/// Create table if it's missing.
#[instrument(skip_all)]
pub async fn check_writability(compute: Arc<ComputeNode>) -> Result<()> {
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(compute.connstr.as_str(), NoTls).await?;
    if client.is_closed() {
        return Err(anyhow!("connection to postgres closed"));
    }

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    let query = "
    CREATE TABLE IF NOT EXISTS health_check (
        id serial primary key,
        updated_at timestamptz default now()
    );
    INSERT INTO health_check VALUES (1, now())
        ON CONFLICT (id) DO UPDATE
         SET updated_at = now();";

    let result = client.simple_query(query).await?;

    if result.len() != 2 {
        return Err(anyhow::format_err!(
            "expected 2 query results, but got {}",
            result.len()
        ));
    }

    Ok(())
}
