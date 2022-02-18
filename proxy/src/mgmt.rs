use crate::{compute::DatabaseInfo, cplane_api};
use anyhow::Context;
use serde::Deserialize;
use std::{
    net::{TcpListener, TcpStream},
    thread,
};
use zenith_utils::{
    postgres_backend::{self, AuthType, PostgresBackend},
    pq_proto::{BeMessage, SINGLE_COL_ROWDESC},
};

///
/// Main proxy listener loop.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        println!("mgmt has shut down");
    }

    listener
        .set_nonblocking(false)
        .context("failed to set listener to blocking")?;
    loop {
        let (socket, peer_addr) = listener.accept().context("failed to accept a new client")?;
        println!("accepted connection from {}", peer_addr);
        socket
            .set_nodelay(true)
            .context("failed to set client socket option")?;

        thread::spawn(move || {
            if let Err(err) = handle_connection(socket) {
                println!("error: {}", err);
            }
        });
    }
}

fn handle_connection(socket: TcpStream) -> anyhow::Result<()> {
    let pgbackend = PostgresBackend::new(socket, AuthType::Trust, None, true)?;
    pgbackend.run(&mut MgmtHandler)
}

struct MgmtHandler;

/// Serialized examples:
// {
//     "session_id": "71d6d03e6d93d99a",
//     "result": {
//         "Success": {
//             "host": "127.0.0.1",
//             "port": 5432,
//             "dbname": "stas",
//             "user": "stas",
//             "password": "mypass"
//         }
//     }
// }
// {
//     "session_id": "71d6d03e6d93d99a",
//     "result": {
//         "Failure": "oops"
//     }
// }
//
// // to test manually by sending a query to mgmt interface:
// psql -h 127.0.0.1 -p 9999 -c '{"session_id":"4f10dde522e14739","result":{"Success":{"host":"127.0.0.1","port":5432,"dbname":"stas","user":"stas","password":"stas"}}}'
#[derive(Deserialize)]
struct PsqlSessionResponse {
    session_id: String,
    result: PsqlSessionResult,
}

#[derive(Deserialize)]
enum PsqlSessionResult {
    Success(DatabaseInfo),
    Failure(String),
}

impl postgres_backend::Handler for MgmtHandler {
    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: &str,
    ) -> anyhow::Result<()> {
        let res = try_process_query(pgb, query_string);
        // intercept and log error message
        if res.is_err() {
            println!("Mgmt query failed: #{:?}", res);
        }
        res
    }
}

fn try_process_query(pgb: &mut PostgresBackend, query_string: &str) -> anyhow::Result<()> {
    println!("Got mgmt query [redacted]");  // Content contains password, don't print it

    let resp: PsqlSessionResponse = serde_json::from_str(query_string)?;

    use PsqlSessionResult::*;
    let msg = match resp.result {
        Success(db_info) => Ok(db_info),
        Failure(message) => Err(message),
    };

    match cplane_api::notify(&resp.session_id, msg) {
        Ok(()) => {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(b"ok")]))?
                .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        }
        Err(e) => {
            pgb.write_message(&BeMessage::ErrorResponse(&e.to_string()))?;
        }
    }

    Ok(())
}
