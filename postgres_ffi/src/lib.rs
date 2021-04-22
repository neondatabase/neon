#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use bytes::{Buf, Bytes, BytesMut};

// sizeof(ControlFileData)
const SIZEOF_CONTROLDATA: usize = std::mem::size_of::<ControlFileData>();
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
