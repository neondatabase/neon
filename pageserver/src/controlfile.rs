#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use bytes::{Buf, Bytes};

use log::*;

pub fn parse_controlfile(b: Bytes) {
    let controlfile = postgres_ffi::decode_pg_control(b);

    info!(
        "controlfile {:X}/{:X}",
        controlfile.checkPoint >> 32,
        controlfile.checkPoint
    );
    info!("controlfile {:?}", controlfile);
}

const MAX_MAPPINGS: usize = 62;

#[derive(Debug)]
struct RelMapping {
    mapoid: u32,      /* OID of a catalog */
    mapfilenode: u32, /* its filenode number */
}

#[derive(Debug)]
pub struct RelMapFile {
    magic: i32,        /* always RELMAPPER_FILEMAGIC */
    num_mappings: i32, /* number of valid RelMapping entries */
    mappings: [u8; MAX_MAPPINGS * 8],
    crc: u32, /* CRC of all above */
    pad: i32, /* to make the struct size be 512 exactly */
}

pub fn decode_filemapping(mut buf: Bytes) -> RelMapFile {
    info!("decode filemap");

    let file: RelMapFile = RelMapFile {
        magic: buf.get_i32_le(),        /* always RELMAPPER_FILEMAGIC */
        num_mappings: buf.get_i32_le(), /* number of valid RelMapping entries */
        mappings: {
            let mut arr = [0 as u8; MAX_MAPPINGS * 8];
            buf.copy_to_slice(&mut arr);
            arr
        },
        crc: buf.get_u32_le(), /* CRC of all above */
        pad: buf.get_i32_le(),
    };

    info!("decode filemap {:?}", file);
    file
}

pub fn write_buf_to_file(filepath: String, buf: Bytes, blkno: u32) {
    info!("write_buf_to_file {}", filepath.clone());

    let mut buffer = File::create(filepath.clone()).unwrap();
    buffer.seek(SeekFrom::Start(8192 * blkno as u64)).unwrap();

    buffer.write_all(&buf).unwrap();

    info!("DONE write_buf_to_file {}", filepath);
}
