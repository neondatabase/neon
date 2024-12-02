use crate::{FromSql, Type};
pub use bytes::BytesMut;
use std::error::Error;

pub fn read_be_i32(buf: &mut &[u8]) -> Result<i32, Box<dyn Error + Sync + Send>> {
    if buf.len() < 4 {
        return Err("invalid buffer size".into());
    }
    let mut bytes = [0; 4];
    bytes.copy_from_slice(&buf[..4]);
    *buf = &buf[4..];
    Ok(i32::from_be_bytes(bytes))
}

pub fn read_value<'a, T>(
    type_: &Type,
    buf: &mut &'a [u8],
) -> Result<T, Box<dyn Error + Sync + Send>>
where
    T: FromSql<'a>,
{
    let len = read_be_i32(buf)?;
    let value = if len < 0 {
        None
    } else {
        if len as usize > buf.len() {
            return Err("invalid buffer size".into());
        }
        let (head, tail) = buf.split_at(len as usize);
        *buf = tail;
        Some(head)
    };
    T::from_sql_nullable(type_, value)
}
