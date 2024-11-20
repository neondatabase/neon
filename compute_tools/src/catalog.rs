use compute_api::responses::CatalogObjects;
use futures::Stream;
use postgres::NoTls;
use std::{path::Path, process::Stdio, result::Result, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    spawn,
};
use tokio_postgres::connect;
use tokio_stream::{self as stream, StreamExt};
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::warn;

use crate::compute::ComputeNode;
use crate::pg_helpers::{get_existing_dbs_async, get_existing_roles_async};

pub async fn get_dbs_and_roles(compute: &Arc<ComputeNode>) -> anyhow::Result<CatalogObjects> {
    let connstr = compute.connstr.clone();

    let (client, connection): (tokio_postgres::Client, _) =
        connect(connstr.as_str(), NoTls).await?;

    spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let roles = get_existing_roles_async(&client).await?;

    let databases = get_existing_dbs_async(&client)
        .await?
        .into_values()
        .collect();

    Ok(CatalogObjects { roles, databases })
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaDumpError {
    #[error("Database does not exist.")]
    DatabaseDoesNotExist,
    #[error("Failed to execute pg_dump.")]
    IO(#[from] std::io::Error),
}

// It uses the pg_dump utility to dump the schema of the specified database.
// The output is streamed back to the caller and supposed to be streamed via HTTP.
//
// Before return the result with the output, it checks that pg_dump produced any output.
// If not, it tries to parse the stderr output to determine if the database does not exist
// and special error is returned.
//
// To make sure that the process is killed when the caller drops the stream, we use tokio kill_on_drop feature.
pub async fn get_database_schema(
    compute: &Arc<ComputeNode>,
    dbname: &str,
) -> Result<impl Stream<Item = Result<bytes::Bytes, std::io::Error>>, SchemaDumpError> {
    let pgbin = &compute.pgbin;
    let basepath = Path::new(pgbin).parent().unwrap();
    let pgdump = basepath.join("pg_dump");
    let mut connstr = compute.connstr.clone();
    connstr.set_path(dbname);
    let mut cmd = Command::new(pgdump)
        .arg("--schema-only")
        .arg(connstr.as_str())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdout = cmd.stdout.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "Failed to capture stdout.")
    })?;

    let stderr = cmd.stderr.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "Failed to capture stderr.")
    })?;

    let mut stdout_reader = FramedRead::new(stdout, BytesCodec::new());
    let stderr_reader = BufReader::new(stderr);

    let first_chunk = match stdout_reader.next().await {
        Some(Ok(bytes)) if !bytes.is_empty() => bytes,
        Some(Err(e)) => {
            return Err(SchemaDumpError::IO(e));
        }
        _ => {
            let mut lines = stderr_reader.lines();
            if let Some(line) = lines.next_line().await? {
                if line.contains(&format!("FATAL:  database \"{}\" does not exist", dbname)) {
                    return Err(SchemaDumpError::DatabaseDoesNotExist);
                }
                warn!("pg_dump stderr: {}", line)
            }
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    warn!("pg_dump stderr: {}", line)
                }
            });

            return Err(SchemaDumpError::IO(std::io::Error::new(
                std::io::ErrorKind::Other,
                "failed to start pg_dump",
            )));
        }
    };
    let initial_stream = stream::once(Ok(first_chunk.freeze()));
    // Consume stderr and log warnings
    tokio::spawn(async move {
        let mut lines = stderr_reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            warn!("pg_dump stderr: {}", line)
        }
    });
    Ok(initial_stream.chain(stdout_reader.map(|res| res.map(|b| b.freeze()))))
}
