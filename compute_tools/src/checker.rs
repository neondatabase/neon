use anyhow::{anyhow, Result};
use tracing::{error, instrument};
use tracing_postgres::{Client, NoTls};

use crate::compute::ComputeNode;

#[instrument(skip_all)]
pub fn create_writability_check_data(client: &mut Client) -> Result<()> {
    let query = "
    CREATE TABLE IF NOT EXISTS health_check (
        id serial primary key,
        updated_at timestamptz default now()
    );
    INSERT INTO health_check VALUES (1, now())
        ON CONFLICT (id) DO UPDATE
         SET updated_at = now();";
    let result = client.simple_query(query)?;
    if result.len() < 2 {
        return Err(anyhow::format_err!("executed  {} queries", result.len()));
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn check_writability(compute: &ComputeNode) -> Result<()> {
    // XXX: There is no tracing wrapper around tokio_postgres. But we have
    // a span for the whole function, that's good enough for now.
    let (client, connection) = tokio_postgres::connect(compute.connstr.as_str(), NoTls).await?;
    if client.is_closed() {
        return Err(anyhow!("connection to postgres closed"));
    }
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    let result = client
        .simple_query("UPDATE health_check SET updated_at = now() WHERE id = 1;")
        .await?;

    if result.len() != 1 {
        return Err(anyhow!("statement can't be executed"));
    }
    Ok(())
}
