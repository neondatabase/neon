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

use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};

// sizeof(ControlFileData)
const SIZEOF_CONTROLDATA: usize = std::mem::size_of::<ControlFileData>();
// offsetof(ControlFileData, crc)
const OFFSETOF_CRC: usize = PG_CONTROLFILEDATA_OFFSETOF_CRC as usize;


///
/// Interpret a slice of bytes as a Postgres control file.
///
pub fn decode_pg_control(buf: &[u8]) -> Result<ControlFileData> {
    // Check that the slice has the expected size. The control file is
    // padded with zeros up to a 512 byte sector size, so accept a
    // larger size too, so that the caller can just the whole file
    // contents without knowing the exact size of the struct.
    if buf.len() < SIZEOF_CONTROLDATA {
        bail!("control file is too short");
    }
    let controlfile: ControlFileData;

    let mut b: [u8; SIZEOF_CONTROLDATA] = [0u8; SIZEOF_CONTROLDATA];
    b.copy_from_slice(&buf[0..SIZEOF_CONTROLDATA]);
    let expectedcrc = crc32c::crc32c(&b[0..OFFSETOF_CRC]);

    controlfile = unsafe { std::mem::transmute::<[u8; SIZEOF_CONTROLDATA], ControlFileData>(b) };

    if expectedcrc != controlfile.crc {
        bail!(
            "invalid CRC in control file: expected {:08X}, was {:08X}",
            expectedcrc,
            controlfile.crc
        );
    }

    Ok(controlfile)
}

///
/// Convert a struct representing a Postgres control file into raw bytes.
///
/// The CRC is recomputed to match the contents of the fields.
pub fn encode_pg_control(controlfile: &ControlFileData) -> Bytes {
    let b: [u8; SIZEOF_CONTROLDATA];

    b = unsafe { std::mem::transmute::<ControlFileData, [u8; SIZEOF_CONTROLDATA]>(*controlfile) };

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
