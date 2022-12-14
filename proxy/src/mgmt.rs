use crate::auth;
use anyhow::Context;
use pq_proto::{BeMessage, SINGLE_COL_ROWDESC};
use serde::Deserialize;
use std::{
    net::{TcpListener, TcpStream},
    thread,
};
use tracing::{error, info, info_span};
use utils::postgres_backend::{self, AuthType, PostgresBackend};

/// Console management API listener thread.
/// It spawns console response handlers needed for the link auth.
pub fn thread_main(listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("mgmt has shut down");
    }

    listener
        .set_nonblocking(false)
        .context("failed to set listener to blocking")?;

    loop {
        let (socket, peer_addr) = listener.accept().context("failed to accept a new client")?;
        info!("accepted connection from {peer_addr}");
        socket
            .set_nodelay(true)
            .context("failed to set client socket option")?;

        // TODO: replace with async tasks.
        thread::spawn(move || {
            let tid = std::thread::current().id();
            let span = info_span!("mgmt", thread = format_args!("{tid:?}"));
            let _enter = span.enter();

            info!("started a new console management API thread");
            scopeguard::defer! {
                info!("console management API thread is about to finish");
            }

            if let Err(e) = handle_connection(socket) {
                error!("thread failed with an error: {e}");
            }
        });
    }
}

fn handle_connection(socket: TcpStream) -> anyhow::Result<()> {
    let pgbackend = PostgresBackend::new(socket, AuthType::Trust, None, true)?;
    pgbackend.run(&mut MgmtHandler)
}

/// Known as `kickResponse` in the console.
#[derive(Debug, Deserialize)]
struct PsqlSessionResponse {
    session_id: String,
    result: PsqlSessionResult,
}

#[derive(Debug, Deserialize)]
enum PsqlSessionResult {
    Success(DatabaseInfo),
    Failure(String),
}

/// A message received by `mgmt` when a compute node is ready.
pub type ComputeReady = Result<DatabaseInfo, String>;

impl PsqlSessionResult {
    fn into_compute_ready(self) -> ComputeReady {
        match self {
            Self::Success(db_info) => Ok(db_info),
            Self::Failure(message) => Err(message),
        }
    }
}

/// Compute node connection params provided by the console.
/// This struct and its parents are mgmt API implementation
/// detail and thus should remain in this module.
// TODO: restore deserialization tests from git history.
#[derive(Deserialize)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    /// Console always provides a password, but it might
    /// be inconvenient for debug with local PG instance.
    pub password: Option<String>,
    pub project: String,
}

// Manually implement debug to omit sensitive info.
impl std::fmt::Debug for DatabaseInfo {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("DatabaseInfo")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("dbname", &self.dbname)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

// TODO: replace with an http-based protocol.
struct MgmtHandler;
impl postgres_backend::Handler for MgmtHandler {
    fn process_query(&mut self, pgb: &mut PostgresBackend, query: &str) -> anyhow::Result<()> {
        try_process_query(pgb, query).map_err(|e| {
            error!("failed to process response: {e:?}");
            e
        })
    }
}

fn try_process_query(pgb: &mut PostgresBackend, query: &str) -> anyhow::Result<()> {
    let resp: PsqlSessionResponse = serde_json::from_str(query)?;

    let span = info_span!("event", session_id = resp.session_id);
    let _enter = span.enter();
    info!("got response: {:?}", resp.result);

    match auth::backend::notify(&resp.session_id, resp.result.into_compute_ready()) {
        Ok(()) => {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(b"ok")]))?
                .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        }
        Err(e) => {
            error!("failed to deliver response to per-client task");
            pgb.write_message(&BeMessage::ErrorResponse(&e.to_string()))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_db_info() -> anyhow::Result<()> {
        // with password
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "password": "password",
            "project": "hello_world",
        }))?;

        // without password
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "project": "hello_world",
        }))?;

        // new field (forward compatibility)
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "project": "hello_world",
            "N.E.W": "forward compatibility check",
        }))?;

        Ok(())
    }
}
