use anyhow::{anyhow, Ok, Result};
use tokio_postgres::NoTls;
use tracing::{error, instrument, warn};

use crate::compute::ComputeNode;

/// Update timestamp in a row in a special service table to check
/// that we can actually write some data in this particular timeline.
#[instrument(skip_all)]
pub async fn check_writability(compute: &ComputeNode) -> Result<()> {
    // Connect to the database.
    let conf = compute.get_tokio_conn_conf(Some("compute_ctl:availability_checker"));
    let (client, connection) = conf.connect(NoTls).await?;
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
    INSERT INTO health_check VALUES (1, now())
        ON CONFLICT (id) DO UPDATE
         SET updated_at = now();";

    match client.simple_query(query).await {
        Result::Ok(result) => {
            if result.len() != 1 {
                return Err(anyhow::anyhow!(
                    "expected 1 query results, but got {}",
                    result.len()
                ));
            }
        }
        Err(err) => {
            if let Some(state) = err.code() {
                if state == &tokio_postgres::error::SqlState::DISK_FULL {
                    warn!("Tenant disk is full");
                    return Ok(());
                }
            }
            return Err(err.into());
        }
    }

    Ok(())
}
