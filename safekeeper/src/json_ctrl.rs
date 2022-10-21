//!
//! This module implements JSON_CTRL protocol, which allows exchange
//! JSON messages over psql for testing purposes.
//!
//! Currently supports AppendLogicalMessage, which is used for WAL
//! modifications in tests.
//!

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::*;
use utils::id::TenantTimelineId;

use crate::handler::SafekeeperPostgresHandler;
use crate::safekeeper::{AcceptorProposerMessage, AppendResponse, ServerInfo};
use crate::safekeeper::{
    AppendRequest, AppendRequestHeader, ProposerAcceptorMessage, ProposerElected,
};
use crate::safekeeper::{SafeKeeperState, Term, TermHistory, TermSwitchEntry};
use crate::timeline::Timeline;
use crate::GlobalTimelines;
use postgres_ffi::encode_logical_message;
use postgres_ffi::WAL_SEGMENT_SIZE;
use utils::{
    lsn::Lsn,
    postgres_backend::PostgresBackend,
    pq_proto::{BeMessage, RowDescriptor, TEXT_OID},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendLogicalMessage {
    // prefix and message to build LogicalMessage
    lm_prefix: String,
    lm_message: String,

    // if true, commit_lsn will match flush_lsn after append
    set_commit_lsn: bool,

    // if true, ProposerElected will be sent before append
    send_proposer_elected: bool,

    // fields from AppendRequestHeader
    term: Term,
    epoch_start_lsn: Lsn,
    begin_lsn: Lsn,
    truncate_lsn: Lsn,
    pg_version: u32,
}

#[derive(Serialize, Deserialize)]
struct AppendResult {
    // safekeeper state after append
    state: SafeKeeperState,
    // info about new record in the WAL
    inserted_wal: InsertedWAL,
}

/// Handles command to craft logical message WAL record with given
/// content, and then append it with specified term and lsn. This
/// function is used to test safekeepers in different scenarios.
pub fn handle_json_ctrl(
    spg: &SafekeeperPostgresHandler,
    pgb: &mut PostgresBackend,
    append_request: &AppendLogicalMessage,
) -> Result<()> {
    info!("JSON_CTRL request: {:?}", append_request);

    // need to init safekeeper state before AppendRequest
    let tli = prepare_safekeeper(spg.ttid, append_request.pg_version)?;

    // if send_proposer_elected is true, we need to update local history
    if append_request.send_proposer_elected {
        send_proposer_elected(&tli, append_request.term, append_request.epoch_start_lsn)?;
    }

    let inserted_wal = append_logical_message(&tli, append_request)?;
    let response = AppendResult {
        state: tli.get_state().1,
        inserted_wal,
    };
    let response_data = serde_json::to_vec(&response)?;

    pgb.write_message_noflush(&BeMessage::RowDescription(&[RowDescriptor {
        name: b"json",
        typoid: TEXT_OID,
        typlen: -1,
        ..Default::default()
    }]))?
    .write_message_noflush(&BeMessage::DataRow(&[Some(&response_data)]))?
    .write_message(&BeMessage::CommandComplete(b"JSON_CTRL"))?;
    Ok(())
}

/// Prepare safekeeper to process append requests without crashes,
/// by sending ProposerGreeting with default server.wal_seg_size.
fn prepare_safekeeper(ttid: TenantTimelineId, pg_version: u32) -> Result<Arc<Timeline>> {
    GlobalTimelines::create(
        ttid,
        ServerInfo {
            pg_version,
            wal_seg_size: WAL_SEGMENT_SIZE as u32,
            system_id: 0,
        },
        Lsn::INVALID,
        Lsn::INVALID,
    )
}

fn send_proposer_elected(tli: &Arc<Timeline>, term: Term, lsn: Lsn) -> Result<()> {
    // add new term to existing history
    let history = tli.get_state().1.acceptor_state.term_history;
    let history = history.up_to(lsn.checked_sub(1u64).unwrap());
    let mut history_entries = history.0;
    history_entries.push(TermSwitchEntry { term, lsn });
    let history = TermHistory(history_entries);

    let proposer_elected_request = ProposerAcceptorMessage::Elected(ProposerElected {
        term,
        start_streaming_at: lsn,
        term_history: history,
        timeline_start_lsn: lsn,
    });

    tli.process_msg(&proposer_elected_request)?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct InsertedWAL {
    begin_lsn: Lsn,
    end_lsn: Lsn,
    append_response: AppendResponse,
}

/// Extend local WAL with new LogicalMessage record. To do that,
/// create AppendRequest with new WAL and pass it to safekeeper.
fn append_logical_message(tli: &Arc<Timeline>, msg: &AppendLogicalMessage) -> Result<InsertedWAL> {
    let wal_data = encode_logical_message(&msg.lm_prefix, &msg.lm_message);
    let sk_state = tli.get_state().1;

    let begin_lsn = msg.begin_lsn;
    let end_lsn = begin_lsn + wal_data.len() as u64;

    let commit_lsn = if msg.set_commit_lsn {
        end_lsn
    } else {
        sk_state.commit_lsn
    };

    let append_request = ProposerAcceptorMessage::AppendRequest(AppendRequest {
        h: AppendRequestHeader {
            term: msg.term,
            epoch_start_lsn: begin_lsn,
            begin_lsn,
            end_lsn,
            commit_lsn,
            truncate_lsn: msg.truncate_lsn,
            proposer_uuid: [0u8; 16],
        },
        wal_data: Bytes::from(wal_data),
    });

    let response = tli.process_msg(&append_request)?;

    let append_response = match response {
        Some(AcceptorProposerMessage::AppendResponse(resp)) => resp,
        _ => anyhow::bail!("not AppendResponse"),
    };

    Ok(InsertedWAL {
        begin_lsn,
        end_lsn,
        append_response,
    })
}
