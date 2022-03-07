// Utilities for reading and writing Values
use std::io::{Error, Write};
use std::os::unix::fs::FileExt;

use bookfile::BoundedReader;

pub fn read_blob<F: FileExt>(file: &F, off: u64) -> Result<Vec<u8>, Error> {
    // read length
    let mut len_buf = [0u8; 4];
    file.read_exact_at(&mut len_buf, off)?;

    let len = u32::from_ne_bytes(len_buf);

    let mut buf: Vec<u8> = Vec::new();
    buf.resize(len as usize, 0);
    file.read_exact_at(&mut buf.as_mut_slice(), off + 4)?;

    Ok(buf)
}

pub fn read_blob_from_chapter<F: FileExt>(
    file: &BoundedReader<&F>,
    off: u64,
) -> Result<Vec<u8>, Error> {
    // read length
    let mut len_buf = [0u8; 4];
    file.read_exact_at(&mut len_buf, off)?;

    let len = u32::from_ne_bytes(len_buf);

    let mut buf: Vec<u8> = Vec::new();
    buf.resize(len as usize, 0);
    file.read_exact_at(&mut buf.as_mut_slice(), off + 4)?;

    Ok(buf)
}

pub fn write_blob<W: Write>(writer: &mut W, buf: &[u8]) -> Result<u64, Error> {
    let val_len = buf.len() as u32;

    // write the 'length' field and kind byte.
    let lenbuf = u32::to_ne_bytes(val_len);

    writer.write_all(&lenbuf)?;
    writer.write_all(buf)?;

    Ok(4 + val_len as u64)
}
