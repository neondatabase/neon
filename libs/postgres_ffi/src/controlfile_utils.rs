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
//! and endianness information. (The PostgreSQL on-disk file format is
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
use super::bindings::{ControlFileData, PG_CONTROL_FILE_SIZE};

use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};

/// Equivalent to sizeof(ControlFileData) in C
const SIZEOF_CONTROLDATA: usize = size_of::<ControlFileData>();

impl ControlFileData {
    /// Compute the offset of the `crc` field within the `ControlFileData` struct.
    /// Equivalent to offsetof(ControlFileData, crc) in C.
    // Someday this can be const when the right compiler features land.
    fn pg_control_crc_offset() -> usize {
        memoffset::offset_of!(ControlFileData, crc)
    }

    ///
    /// Interpret a slice of bytes as a Postgres control file.
    ///
    pub fn decode(buf: &[u8]) -> Result<ControlFileData> {
        use utils::bin_ser::LeSer;

        // Check that the slice has the expected size. The control file is
        // padded with zeros up to a 512 byte sector size, so accept a
        // larger size too, so that the caller can just the whole file
        // contents without knowing the exact size of the struct.
        if buf.len() < SIZEOF_CONTROLDATA {
            bail!("control file is too short");
        }

        // Compute the expected CRC of the content.
        let OFFSETOF_CRC = Self::pg_control_crc_offset();
        let expectedcrc = crc32c::crc32c(&buf[0..OFFSETOF_CRC]);

        // Use serde to deserialize the input as a ControlFileData struct.
        let controlfile = ControlFileData::des_prefix(buf)?;

        // Check the CRC
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
    pub fn encode(&self) -> Bytes {
        use utils::bin_ser::LeSer;

        // Serialize into a new buffer.
        let b = self.ser().unwrap();

        // Recompute the CRC
        let OFFSETOF_CRC = Self::pg_control_crc_offset();
        let newcrc = crc32c::crc32c(&b[0..OFFSETOF_CRC]);

        let mut buf = BytesMut::with_capacity(PG_CONTROL_FILE_SIZE as usize);
        buf.extend_from_slice(&b[0..OFFSETOF_CRC]);
        buf.extend_from_slice(&newcrc.to_ne_bytes());
        // Fill the rest of the control file with zeros.
        buf.resize(PG_CONTROL_FILE_SIZE as usize, 0);

        buf.into()
    }
}
