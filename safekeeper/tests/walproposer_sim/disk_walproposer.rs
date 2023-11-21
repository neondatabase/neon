use std::{ffi::{CStr, CString}, sync::Arc};

use byteorder::{WriteBytesExt, BigEndian};
use parking_lot::{Mutex, MutexGuard};
use utils::lsn::Lsn;
use postgres_ffi::v16::{xlog_utils::XlLogicalMessage, XLogRecord};

use super::disk::BlockStorage;

pub struct DiskWalProposer {
    state: Mutex<State>,
}

impl DiskWalProposer {
    pub fn new() -> Arc<DiskWalProposer> {
        Arc::new(DiskWalProposer {
            state: Mutex::new(State {
                write_lsn: Lsn(0),
                disk: BlockStorage::new(),
            }),
        })
    }

    pub fn lock(&self) -> MutexGuard<State> {
        self.state.lock()
    }
}

pub struct State {
    // TODO: add horizon_lsn
    write_lsn: Lsn,
    disk: BlockStorage,
}

impl State {
    pub fn read(&self, pos: u64, mut buf: &mut [u8]) {
        self.disk.read(pos, buf);
        // TODO: fail on reading uninitialized data
    }

    pub fn insert_logical_message(&mut self, prefix: &str, msg: &[u8]) -> anyhow::Result<()> {
        let mut insert = InsertLM{
            num_rdata: 0,
            mainrdata_len: 0,
            lsn: self.write_lsn,
            disk: &mut self.disk,
            scratch: Vec::new(),
        };

        let prefix_cstr = CString::new(prefix)?;
        let prefix_bytes = prefix_cstr.as_bytes_with_nul();

        let lm = XlLogicalMessage {
            db_id: 0,
            transactional: 0,
            prefix_size: prefix_bytes.len() as ::std::os::raw::c_ulong,
            message_size: msg.len() as ::std::os::raw::c_ulong,
        };

        let wal_record = lm.encode();

        // TODO: assert(wal_record.len() == SizeOfXLogicalMessage)

        insert.register_data(&wal_record);
        insert.register_data(prefix_bytes);
        insert.register_data(msg);

        // return MyFinishInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, XLOG_INCLUDE_ORIGIN);
        insert.finish(21, 0, 1);

        todo!("update lsn");
        Ok(())
    }
}

struct InsertLM<'a> {
    num_rdata: usize,
    mainrdata_len: usize,
    lsn: Lsn,
    disk: &'a mut BlockStorage,
    scratch: Vec<u8>,
}

impl<'a> InsertLM<'a> {
    fn register_data(&mut self, data: &[u8]) {
        self.mainrdata_len += data.len();
        todo!()
    }

    fn finish(&mut self, rmid: u8, info: u8, flags: u8) -> anyhow::Result<()> {
        const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
        const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;

        let rec = XLogRecord::default();
        let rec_bytes = rec.encode()?;
        self.scratch.extend_from_slice(&rec_bytes);

        if self.mainrdata_len > 0 {
            if self.mainrdata_len > 255 {
                self.scratch.push(XLR_BLOCK_ID_DATA_LONG);
                // TODO: verify order, endiness
                self.scratch.write_u32::<BigEndian>(self.mainrdata_len as u32);
            } else {
                self.scratch.push(XLR_BLOCK_ID_DATA_SHORT);
                self.scratch.push(self.mainrdata_len as u8);
            }
        }

        let total_len: u32 = (self.mainrdata_len + self.scratch.len()) as u32;

        
        todo!()
    }
}
