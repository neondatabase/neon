//!
//! This module implements JSON_CTRL protocol, which allows exchange
//! JSON messages over psql for testing purposes.
//!
//! Currently supports AppendLogicalMessage, which is used for WAL
//! modifications in tests.
//!

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use crc32c::crc32c_append;
use log::*;
use serde::{Deserialize, Serialize};

use crate::safekeeper::{AcceptorProposerMessage, AppendResponse};
use crate::safekeeper::{AppendRequest, AppendRequestHeader, ProposerAcceptorMessage, ProposerGreeting};
use crate::safekeeper::{SafeKeeperState, Term};
use crate::send_wal::SendWalHandler;
use crate::timeline::TimelineTools;
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils;
use postgres_ffi::{uint32, uint64, Oid, XLogRecord};
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, RowDescriptor, TEXT_OID};

#[derive(Serialize, Deserialize, Debug)]
struct AppendLogicalMessage {
    // prefix and message to build LogicalMessage
    lm_prefix: String,
    lm_message: String,

    // if true, commit_lsn will match flush_lsn after append
    set_commit_lsn: bool,

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

pub fn handle_json_ctrl(
    swh: &mut SendWalHandler,
    pgb: &mut PostgresBackend,
    cmd: &Bytes,
) -> Result<()> {
    let cmd = cmd
        .strip_prefix(b"JSON_CTRL")
        .ok_or_else(|| anyhow!("invalid prefix"))?;
    // trim zeroes in the end
    let cmd = cmd.strip_suffix(&[0u8]).unwrap_or(cmd);

    let append_request: AppendLogicalMessage = serde_json::from_slice(cmd)?;
    info!("JSON_CTRL request: {:?}", append_request);

    // need to init safekeeper state before AppendRequest
    prepare_safekeeper(swh)?;

    let inserted_wal = append_logical_message(swh, append_request)?;
    let response = AppendResult {
        state: swh.timeline.get().get_info(),
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
fn prepare_safekeeper(swh: &mut SendWalHandler) -> Result<()> {
    let greeting_request = ProposerAcceptorMessage::Greeting(ProposerGreeting {
        protocol_version: 1, // current protocol
        pg_version: 0, // unknown
        proposer_id: [0u8; 16],
        system_id: 0,
        ztli: swh.timelineid.unwrap(),
        tenant_id: swh.tenantid.unwrap(),
        tli: 0,
        wal_seg_size: pg_constants::WAL_SEGMENT_SIZE as u32, // 16MB, default for tests
    });

    let response = swh.timeline.get().process_msg(&greeting_request)?;
    match response {
        AcceptorProposerMessage::Greeting(_) => Ok(()),
        _ => return Err(anyhow!("not GreetingResponse")),
    }
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
    swh: &mut SendWalHandler,
    msg: AppendLogicalMessage,
) -> Result<InsertedWAL> {
    let wal_data = encode_logical_message(msg.lm_prefix, msg.lm_message);
    let sk_state = swh.timeline.get().get_info();

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

    let response = swh.timeline.get().process_msg(&append_request)?;

    let append_response = match response {
        AcceptorProposerMessage::AppendResponse(resp) => resp,
        _ => return Err(anyhow!("not AppendResponse")),
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
        use zenith_utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }
}

// Create new WAL record for non-transactional logical message.
// Used for creating artificial WAL for tests, as LogicalMessage
// record is basically no-op.
fn encode_logical_message(prefix: String, message: String) -> Vec<u8> {
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

    let header_bytes = header.encode();
    let crc = crc32c_append(0, &data);
    let crc = crc32c_append(crc, &header_bytes[0..xlog_utils::XLOG_RECORD_CRC_OFFS]);
    header.xl_crc = crc;

    let mut wal: Vec<u8> = Vec::new();
    wal.extend_from_slice(&header.encode());
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
    let actual = encode_logical_message("prefix".to_string(), "message".to_string());
    assert_eq!(expected, actual[..]);
}
