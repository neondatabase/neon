//!
//! Utilities for reading and writing the PostgreSQL control file.
//!
//! The PostgreSQL control file is one the first things that the PostgreSQL
//! server reads when it starts up. It indicates whether the server was shut
//! down cleanly, or if it crashed or was restored from online backup so that
//! WAL recovery needs to be performed. It also contains a copy of the latest
//! checkpoint record and its location in the WAL.
//!
//! The control file also contains fields for detecting whether the
//! data directory is compatible with a postgres binary. That includes
//! a version number, configuration options that can be set at
//! compilation time like the block size, and the platform's alignment
//! and endianess information. (The PostgreSQL on-disk file format is
//! not portable across platforms.)
//!
//! The control file is stored in the PostgreSQL data directory, as
//! `global/pg_control`. The data stored in it is designed to be smaller than
//! 512 bytes, on the assumption that it can be updated atomically. The actual
//! file is larger, 8192 bytes, but the rest of it is just filled with zeros.
//!
//! See src/include/catalog/pg_control.h in the PostgreSQL sources for more
//! information. You can use PostgreSQL's pg_controldata utility to view its
//! contents.
//!
use crate::{ControlFileData, PG_CONTROLFILEDATA_OFFSETOF_CRC, PG_CONTROL_FILE_SIZE};

use bytes::{Buf, Bytes, BytesMut};

// sizeof(ControlFileData)
const SIZEOF_CONTROLDATA: usize = std::mem::size_of::<ControlFileData>();
// offsetof(ControlFileData, crc)
const OFFSETOF_CRC: usize = PG_CONTROLFILEDATA_OFFSETOF_CRC as usize;

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
