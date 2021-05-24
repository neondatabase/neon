#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub mod nonrelfile_utils;
pub mod pg_constants;
pub mod relfile_utils;
pub mod xlog_utils;

use bytes::{Buf, Bytes, BytesMut};

// sizeof(ControlFileData)
const SIZEOF_CONTROLDATA: usize = std::mem::size_of::<ControlFileData>();
const SIZEOF_CHECKPOINT: usize = std::mem::size_of::<CheckPoint>();
const OFFSETOF_CRC: usize = PG_CONTROLFILEDATA_OFFSETOF_CRC as usize;

impl ControlFileData {
    // Initialize an all-zeros ControlFileData struct
    pub fn new() -> ControlFileData {
        let controlfile: ControlFileData;

        let b = [0u8; SIZEOF_CONTROLDATA];
        controlfile =
            unsafe { std::mem::transmute::<[u8; SIZEOF_CONTROLDATA], ControlFileData>(b) };

        controlfile
    }
}

pub fn decode_pg_control(mut buf: Bytes) -> Result<ControlFileData, anyhow::Error> {
    let mut b: [u8; SIZEOF_CONTROLDATA] = [0u8; SIZEOF_CONTROLDATA];
    buf.copy_to_slice(&mut b);

    let controlfile: ControlFileData;

    // TODO: verify CRC
    let mut data_without_crc: [u8; OFFSETOF_CRC] = [0u8; OFFSETOF_CRC];
    data_without_crc.copy_from_slice(&b[0..OFFSETOF_CRC]);
    let expectedcrc = crc32c::crc32c(&data_without_crc);

    controlfile = unsafe { std::mem::transmute::<[u8; SIZEOF_CONTROLDATA], ControlFileData>(b) };

    if expectedcrc != controlfile.crc {
        anyhow::bail!(
            "invalid CRC in control file: expected {:08X}, was {:08X}",
            expectedcrc,
            controlfile.crc
        );
    }

    Ok(controlfile)
}

pub fn encode_pg_control(controlfile: ControlFileData) -> Bytes {
    let b: [u8; SIZEOF_CONTROLDATA];

    b = unsafe { std::mem::transmute::<ControlFileData, [u8; SIZEOF_CONTROLDATA]>(controlfile) };

    // Recompute the CRC
    let mut data_without_crc: [u8; OFFSETOF_CRC] = [0u8; OFFSETOF_CRC];
    data_without_crc.copy_from_slice(&b[0..OFFSETOF_CRC]);
    let newcrc = crc32c::crc32c(&data_without_crc);

    let mut buf = BytesMut::with_capacity(PG_CONTROL_FILE_SIZE as usize);

    buf.extend_from_slice(&b[0..OFFSETOF_CRC]);
    buf.extend_from_slice(&newcrc.to_ne_bytes());
    // Fill the rest of the control file with zeros.
    buf.resize(PG_CONTROL_FILE_SIZE as usize, 0);

    buf.into()
}

pub fn encode_checkpoint(checkpoint: CheckPoint) -> Bytes {
    let b: [u8; SIZEOF_CHECKPOINT];
    b = unsafe { std::mem::transmute::<CheckPoint, [u8; SIZEOF_CHECKPOINT]>(checkpoint) };
    Bytes::copy_from_slice(&b[..])
}

pub fn decode_checkpoint(mut buf: Bytes) -> Result<CheckPoint, anyhow::Error> {
    let mut b = [0u8; SIZEOF_CHECKPOINT];
    buf.copy_to_slice(&mut b);
    let checkpoint: CheckPoint;
    checkpoint = unsafe { std::mem::transmute::<[u8; SIZEOF_CHECKPOINT], CheckPoint>(b) };
    Ok(checkpoint)
}

impl CheckPoint {
    pub fn new(lsn: u64, timeline: u32) -> CheckPoint {
        CheckPoint {
            redo: lsn,
            ThisTimeLineID: timeline,
            PrevTimeLineID: timeline,
            fullPageWrites: true, // TODO: get actual value of full_page_writes
            nextXid: FullTransactionId {
                value: pg_constants::FIRST_NORMAL_TRANSACTION_ID as u64,
            }, // TODO: handle epoch?
            nextOid: pg_constants::FIRST_BOOTSTRAP_OBJECT_ID,
            nextMulti: 1,
            nextMultiOffset: 0,
            oldestXid: pg_constants::FIRST_NORMAL_TRANSACTION_ID,
            oldestXidDB: 0,
            oldestMulti: 1,
            oldestMultiDB: 0,
            time: 0,
            oldestCommitTsXid: 0,
            newestCommitTsXid: 0,
            oldestActiveXid: pg_constants::INVALID_TRANSACTION_ID,
        }
    }
}
