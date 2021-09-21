//!
//! This module implements JSON_CTRL protocol, which allows exchange
//! JSON messages over psql for testing purposes.
//!
//! Currently supports AppendLogicalMessage, which is used for WAL
//! modifications in tests.
//!

use std::cmp::min;
use std::fmt::Write;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use crc32c::*;
use log::*;
use serde::{Deserialize, Serialize};
use serde_json;

use crate::replication::ReplicationConn;
use crate::safekeeper::SafeKeeperState;
use crate::safekeeper::{AcceptorProposerMessage, AppendResponse};
use crate::safekeeper::{AppendRequest, AppendRequestHeader, ProposerAcceptorMessage};
use crate::send_wal::SendWalHandler;
use crate::timeline::TimelineTools;
use pageserver::waldecoder::{decode_wal_record, WalStreamDecoder};
use postgres_ffi::xlog_utils;
use postgres_ffi::pg_constants;
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
}

#[derive(Serialize, Deserialize)]
struct AppendResult {
    // safekeeper state after append
    state: SafeKeeperState,
    wal_hex: String,
    // info about new record in the WAL
    inserted_wal: InsertedWAL,
}

pub fn handle_json_ctrl(
    swh: &mut SendWalHandler,
    pgb: &mut PostgresBackend,
    cmd: &Bytes,
) -> Result<()> {
    let cmd = cmd.strip_prefix(b"JSON_CTRL");
    if cmd.is_none() {
        return Err(anyhow!("invalid prefix"));
    }
    let cmd = cmd.unwrap();
    // trim zeroes in the end
    let cmd = cmd.strip_suffix(&[0u8]).unwrap_or(cmd);

    let append_request: AppendLogicalMessage = serde_json::from_slice(cmd)?;
    info!("JSON_CTRL request: {:?}", append_request);

    let inserted_wal = handle_append_logical_message(swh, append_request)?;

    let response = AppendResult {
        state: swh.timeline.get().get_info(),
        wal_hex: pretty_hex_wal(swh)?,
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

#[derive(Serialize, Deserialize)]
struct InsertedWAL {
    start_lsn: Lsn,
    end_lsn: Lsn,
    append_response: AppendResponse,
}

/// Extend local WAL with new LogicalMessage record. To do that,
/// create AppendRequest with new WAL and pass it to safekeeper.
fn handle_append_logical_message(
    swh: &mut SendWalHandler,
    msg: AppendLogicalMessage,
) -> Result<InsertedWAL> {
    let (wal_end, _) = swh.timeline.get().get_end_of_wal();
    let sk_state = swh.timeline.get().get_info();

    let wal_data = encode_logical_message(msg.lm_prefix, msg.lm_message);
    let lsn_after_append = wal_end + wal_data.len() as u64;

    let commit_lsn = if msg.set_commit_lsn {
        lsn_after_append
    } else {
        sk_state.commit_lsn
    };

    let append_request = ProposerAcceptorMessage::AppendRequest(AppendRequest {
        h: AppendRequestHeader {
            term: sk_state.acceptor_state.term + 1,
            epoch_start_lsn: wal_end,
            begin_lsn: wal_end,
            end_lsn: lsn_after_append,
            commit_lsn: commit_lsn,
            truncate_lsn: sk_state.truncate_lsn,
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
        start_lsn: wal_end,
        end_lsn: lsn_after_append,
        append_response,
    })
}

#[repr(C)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct XlLogicalMessage {
    db_id: Oid,
    transactional: uint32, // 4 bytes bool?
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
        db_id: 14236, // TODO: fetch db_id from somewhere?
        transactional: 0,
        prefix_size: prefix_bytes.len() as u64,
        message_size: message_bytes.len() as u64,
    };

    let mainrdata = logical_message.encode();
    let mainrdata_len: usize = mainrdata.len() + prefix_bytes.len() + message_bytes.len();
    // only short mainrdata is supported for now
    assert!(mainrdata_len <= 255);
    let mainrdata_len: u8 = mainrdata_len as u8;

    let mut data: Vec<u8> = Vec::new();
    data.push(pg_constants::XLR_BLOCK_ID_DATA_SHORT);
    data.push(mainrdata_len);
    data.extend_from_slice(&mainrdata);
    data.extend_from_slice(&prefix_bytes);
    data.extend_from_slice(&message_bytes);

    let total_len = xlog_utils::XLOG_SIZE_OF_XLOG_RECORD + data.len();

    let mut header = XLogRecord {
        xl_tot_len: total_len as u32,
        xl_xid: 0,
        xl_prev: 0, // TODO: fill prev lsn?
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

    const PADDING: usize = 8;
    let padding_rem = wal.len() % PADDING;
    if padding_rem != 0 {
        wal.resize(wal.len() + PADDING - padding_rem, 0);
    }

    wal
}

#[test]
fn test_encode_logical_message() {
    // TODO: assert_eq the result of encoding
    encode_logical_message("".to_string(), "12345679".to_string());
}

fn pretty_hex_wal(swh: &mut SendWalHandler) -> Result<String> {
    let wal_info = retrieve_wal_info(swh)?;

    let mut decoder = WalStreamDecoder::new(wal_info.start_pos);
    decoder.feed_bytes(&wal_info.wal);
    loop {
        let rec = decoder.poll_decode();
        if rec.is_err() {
            break;
        }
        if let Some((lsn, recdata)) = rec.unwrap() {
            let record = decode_wal_record(recdata);
            println!("found wal record, next_lsn={} xl_xid={} xl_info={} xl_rmid={} bytes={:?} offset={}", lsn, record.xl_xid, record.xl_info, record.xl_rmid, record.record, record.main_data_offset);
        } else {
            break;
        }
    }

    let mut wal_hex = String::new();
    for byte in wal_info.wal {
        write!(&mut wal_hex, "{:X}", byte).expect("Unable to write");
    }

    Ok(wal_hex)
}

struct WalInfo {
    wal: Vec<u8>,
    start_pos: Lsn,
}

// TODO: acquire mutex, read WAL from commit_lsn till end, parse WAL
fn retrieve_wal_info(swh: &mut SendWalHandler) -> Result<WalInfo> {
    let (wal_end, timeline_id) = swh.timeline.get().get_end_of_wal();
    let mut wal: Vec<u8> = Vec::new();

    let timeline_info = swh.timeline.get().get_info();

    let mut start_pos = timeline_info.commit_lsn;
    let stop_pos = wal_end;

    let wal_seg_size = timeline_info.server.wal_seg_size as usize;
    let mut wal_file: Option<File> = None;

    while start_pos < stop_pos {
        // Take the `File` from `wal_file`, or open a new file.
        let mut file = match wal_file.take() {
            Some(file) => file,
            None => {
                // Open a new file.
                let segno = start_pos.segment_number(wal_seg_size);
                let wal_file_name = xlog_utils::XLogFileName(timeline_id, segno, wal_seg_size);
                let timeline_id = swh.timeline.get().timelineid.to_string();
                let wal_file_path = swh.conf.data_dir.join(timeline_id).join(wal_file_name);
                ReplicationConn::open_wal_file(&wal_file_path)?
            }
        };

        let xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

        // How much to read and send in message? We cannot cross the WAL file
        // boundary, and we don't want send more than MAX_SEND_SIZE.
        let send_size = stop_pos.checked_sub(start_pos).unwrap().0 as usize;
        let send_size = min(send_size, wal_seg_size - xlogoff);
        let send_size = min(send_size, xlog_utils::MAX_SEND_SIZE);

        // Read some data from the file.
        let mut file_buf = vec![0u8; send_size];
        file.seek(SeekFrom::Start(xlogoff as u64))
            .and_then(|_| file.read_exact(&mut file_buf))
            .context("Failed to read data from WAL file")?;

        wal.extend(&file_buf);
        start_pos += send_size as u64;

        // Decide whether to reuse this file. If we don't set wal_file here
        // a new file will be opened next time.
        if start_pos.segment_offset(wal_seg_size) != 0 {
            wal_file = Some(file);
        }
    }

    Ok(WalInfo {
        wal,
        start_pos: timeline_info.commit_lsn,
    })
}
