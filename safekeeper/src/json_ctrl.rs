//!
//! This module implements JSON_CTRL protocol, which allows exchange
//! JSON messages over psql for testing purposes.
//!
//! Currently supports AppendLogicalMessage, which is used for WAL
//! modifications in tests.
//!

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use crc32c::crc32c_append;
use serde::{Deserialize, Serialize};
use tracing::*;

use crate::handler::SafekeeperPostgresHandler;
use crate::safekeeper::{AcceptorProposerMessage, AppendResponse};
use crate::safekeeper::{
    AppendRequest, AppendRequestHeader, ProposerAcceptorMessage, ProposerElected, ProposerGreeting,
};
use crate::safekeeper::{SafeKeeperState, Term, TermHistory, TermSwitchEntry};
use crate::timeline::TimelineTools;
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils;
use postgres_ffi::{uint32, uint64, Oid, XLogRecord};
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
    spg: &mut SafekeeperPostgresHandler,
    pgb: &mut PostgresBackend,
    append_request: &AppendLogicalMessage,
) -> Result<()> {
    info!("JSON_CTRL request: {:?}", append_request);

    // need to init safekeeper state before AppendRequest
    prepare_safekeeper(spg)?;

    // if send_proposer_elected is true, we need to update local history
    if append_request.send_proposer_elected {
        send_proposer_elected(spg, append_request.term, append_request.epoch_start_lsn)?;
    }

    let inserted_wal = append_logical_message(spg, append_request)?;
    let response = AppendResult {
        state: spg.timeline.get().get_state().1,
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
fn prepare_safekeeper(spg: &mut SafekeeperPostgresHandler) -> Result<()> {
    let greeting_request = ProposerAcceptorMessage::Greeting(ProposerGreeting {
        protocol_version: 2, // current protocol
        pg_version: 0,       // unknown
        proposer_id: [0u8; 16],
        system_id: 0,
        ztli: spg.ztimelineid.unwrap(),
        tenant_id: spg.tenantId.unwrap(),
        tli: 0,
        wal_seg_size: pg_constants::WAL_SEGMENT_SIZE as u32, // 16MB, default for tests
    });

    let response = spg.timeline.get().process_msg(&greeting_request)?;
    match response {
        Some(AcceptorProposerMessage::Greeting(_)) => Ok(()),
        _ => anyhow::bail!("not GreetingResponse"),
    }
}

fn send_proposer_elected(spg: &mut SafekeeperPostgresHandler, term: Term, lsn: Lsn) -> Result<()> {
    // add new term to existing history
    let history = spg.timeline.get().get_state().1.acceptor_state.term_history;
    let history = history.up_to(lsn.checked_sub(1u64).unwrap());
    let mut history_entries = history.0;
    history_entries.push(TermSwitchEntry { term, lsn });
    let history = TermHistory(history_entries);

    let proposer_elected_request = ProposerAcceptorMessage::Elected(ProposerElected {
        term,
        start_streaming_at: lsn,
        term_history: history,
        timeline_start_lsn: Lsn(0),
    });

    spg.timeline.get().process_msg(&proposer_elected_request)?;
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
fn append_logical_message(
    spg: &mut SafekeeperPostgresHandler,
    msg: &AppendLogicalMessage,
) -> Result<InsertedWAL> {
    let wal_data = encode_logical_message(&msg.lm_prefix, &msg.lm_message);
    let sk_state = spg.timeline.get().get_state().1;

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

    let response = spg.timeline.get().process_msg(&append_request)?;

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

#[repr(C)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct XlLogicalMessage {
    db_id: Oid,
    transactional: uint32, // bool, takes 4 bytes due to alignment in C structures
    prefix_size: uint64,
    message_size: uint64,
}

impl XlLogicalMessage {
    pub fn encode(&self) -> Bytes {
        use utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }
}

/// Create new WAL record for non-transactional logical message.
/// Used for creating artificial WAL for tests, as LogicalMessage
/// record is basically no-op.
fn encode_logical_message(prefix: &str, message: &str) -> Vec<u8> {
    let mut prefix_bytes = BytesMut::with_capacity(prefix.len() + 1);
    prefix_bytes.put(prefix.as_bytes());
    prefix_bytes.put_u8(0);

    let message_bytes = message.as_bytes();

    let logical_message = XlLogicalMessage {
        db_id: 0,
        transactional: 0,
        prefix_size: prefix_bytes.len() as u64,
        message_size: message_bytes.len() as u64,
    };

    let mainrdata = logical_message.encode();
    let mainrdata_len: usize = mainrdata.len() + prefix_bytes.len() + message_bytes.len();
    // only short mainrdata is supported for now
    assert!(mainrdata_len <= 255);
    let mainrdata_len = mainrdata_len as u8;

    let mut data: Vec<u8> = vec![pg_constants::XLR_BLOCK_ID_DATA_SHORT, mainrdata_len];
    data.extend_from_slice(&mainrdata);
    data.extend_from_slice(&prefix_bytes);
    data.extend_from_slice(message_bytes);

    let total_len = xlog_utils::XLOG_SIZE_OF_XLOG_RECORD + data.len();

    let mut header = XLogRecord {
        xl_tot_len: total_len as u32,
        xl_xid: 0,
        xl_prev: 0,
        xl_info: 0,
        xl_rmid: 21,
        __bindgen_padding_0: [0u8; 2usize],
        xl_crc: 0, // crc will be calculated later
    };

    let header_bytes = header.encode().expect("failed to encode header");
    let crc = crc32c_append(0, &data);
    let crc = crc32c_append(crc, &header_bytes[0..xlog_utils::XLOG_RECORD_CRC_OFFS]);
    header.xl_crc = crc;

    let mut wal: Vec<u8> = Vec::new();
    wal.extend_from_slice(&header.encode().expect("failed to encode header"));
    wal.extend_from_slice(&data);

    // WAL start position must be aligned at 8 bytes,
    // this will add padding for the next WAL record.
    const PADDING: usize = 8;
    let padding_rem = wal.len() % PADDING;
    if padding_rem != 0 {
        wal.resize(wal.len() + PADDING - padding_rem, 0);
    }

    wal
}

#[test]
fn test_encode_logical_message() {
    let expected = [
        64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 170, 34, 166, 227, 255, 38,
        0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 112, 114, 101, 102,
        105, 120, 0, 109, 101, 115, 115, 97, 103, 101,
    ];
    let actual = encode_logical_message("prefix", "message");
    assert_eq!(expected, actual[..]);
}
