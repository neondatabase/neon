//! Frontend message serialization.
#![allow(missing_docs)]

use std::io;

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, BytesMut};

use crate::{CSafeStr, FromUsize, IsNull, Oid, write_nullable};

#[inline]
fn write_body<F>(buf: &mut BytesMut, f: F)
where
    F: FnOnce(&mut BytesMut),
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf);

    let size =
        i32::from_usize(buf.len() - base).expect("buffer size should not be larger than i32::MAX");
    BigEndian::write_i32(&mut buf[base..], size);
}

#[inline]
pub fn bind<I, J, F, T, K>(
    portal: &CSafeStr,
    statement: &CSafeStr,
    formats: I,
    values: J,
    mut serializer: F,
    result_formats: K,
    buf: &mut BytesMut,
) where
    I: IntoIterator<Item = i16>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut) -> IsNull,
    K: IntoIterator<Item = i16>,
{
    buf.put_u8(b'B');

    write_body(buf, |buf| {
        write_cstr(portal, buf);
        write_cstr(statement, buf);
        write_counted(formats, |f, buf| buf.put_i16(f), buf);
        write_counted(
            values,
            |v, buf| write_nullable(|buf| serializer(v, buf), buf),
            buf,
        );
        write_counted(result_formats, |f, buf| buf.put_i16(f), buf);
    })
}

#[inline]
fn write_counted<I, T, F>(items: I, mut serializer: F, buf: &mut BytesMut)
where
    I: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut),
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 2]);
    let mut count = 0;
    for item in items {
        serializer(item, buf);
        count += 1;
    }
    let count = i16::from_usize(count).expect("list should not exceed 32767 items");
    BigEndian::write_i16(&mut buf[base..], count);
}

#[inline]
pub fn cancel_request(process_id: i32, secret_key: i32, buf: &mut BytesMut) {
    write_body(buf, |buf| {
        buf.put_i32(80_877_102);
        buf.put_i32(process_id);
        buf.put_i32(secret_key);
    })
}

#[inline]
pub fn close(variant: u8, name: &CSafeStr, buf: &mut BytesMut) {
    buf.put_u8(b'C');
    write_body(buf, |buf| {
        buf.put_u8(variant);
        write_cstr(name, buf)
    })
}

pub struct CopyData<T> {
    buf: T,
    len: i32,
}

impl<T> CopyData<T>
where
    T: Buf,
{
    pub fn new(buf: T) -> io::Result<CopyData<T>> {
        let len = buf
            .remaining()
            .checked_add(4)
            .and_then(|l| i32::try_from(l).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "message length overflow")
            })?;

        Ok(CopyData { buf, len })
    }

    pub fn write(self, out: &mut BytesMut) {
        out.put_u8(b'd');
        out.put_i32(self.len);
        out.put(self.buf);
    }
}

#[inline]
pub fn copy_done(buf: &mut BytesMut) {
    buf.put_u8(b'c');
    write_body(buf, |_| {});
}

#[inline]
pub fn copy_fail(message: &CSafeStr, buf: &mut BytesMut) {
    buf.put_u8(b'f');
    write_body(buf, |buf| write_cstr(message, buf))
}

#[inline]
pub fn describe(variant: u8, name: &CSafeStr, buf: &mut BytesMut) {
    buf.put_u8(b'D');
    write_body(buf, |buf| {
        buf.put_u8(variant);
        write_cstr(name, buf)
    })
}

#[inline]
pub fn execute(portal: &CSafeStr, max_rows: i32, buf: &mut BytesMut) {
    buf.put_u8(b'E');
    write_body(buf, |buf| {
        write_cstr(portal, buf);
        buf.put_i32(max_rows);
    })
}

#[inline]
pub fn parse<I>(name: &CSafeStr, query: &CSafeStr, param_types: I, buf: &mut BytesMut)
where
    I: IntoIterator<Item = Oid>,
{
    buf.put_u8(b'P');
    write_body(buf, |buf| {
        write_cstr(name, buf);
        write_cstr(query, buf);
        write_counted(param_types, |t, buf| buf.put_u32(t), buf);
    })
}

#[inline]
pub fn password_message(password: &CSafeStr, buf: &mut BytesMut) {
    buf.put_u8(b'p');
    write_body(buf, |buf| write_cstr(password, buf))
}

#[inline]
pub fn query(query: &CSafeStr, buf: &mut BytesMut) {
    buf.put_u8(b'Q');
    write_body(buf, |buf| write_cstr(query, buf))
}

#[inline]
pub fn sasl_initial_response(mechanism: &CSafeStr, data: &[u8], buf: &mut BytesMut) {
    buf.put_u8(b'p');
    write_body(buf, |buf| {
        write_cstr(mechanism, buf);
        let len =
            i32::from_usize(data.len()).expect("sasl data should not be larger than i32::MAX");
        buf.put_i32(len);
        buf.put_slice(data);
    })
}

#[inline]
pub fn sasl_response(data: &[u8], buf: &mut BytesMut) {
    buf.put_u8(b'p');
    write_body(buf, |buf| {
        buf.put_slice(data);
    })
}

#[inline]
pub fn ssl_request(buf: &mut BytesMut) {
    write_body(buf, |buf| {
        buf.put_i32(80_877_103);
    });
}

#[inline]
pub fn startup_message(parameters: &StartupMessageParams, buf: &mut BytesMut) {
    write_body(buf, |buf| {
        // postgres protocol version 3.0(196608) in bigger-endian
        buf.put_i32(0x00_03_00_00);
        buf.put_slice(&parameters.params);
        buf.put_u8(0);
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupMessageParams {
    pub params: BytesMut,
}

impl StartupMessageParams {
    /// Set parameter's value by its name.
    pub fn insert(&mut self, name: &CSafeStr, value: &CSafeStr) {
        self.params.put_slice(name.as_bytes());
        self.params.put_u8(0);
        self.params.put_slice(value.as_bytes());
        self.params.put_u8(0);
    }
}

#[inline]
pub fn sync(buf: &mut BytesMut) {
    buf.put_u8(b'S');
    write_body(buf, |_| {});
}

#[inline]
pub fn terminate(buf: &mut BytesMut) {
    buf.put_u8(b'X');
    write_body(buf, |_| {});
}

#[inline]
fn write_cstr(s: &CSafeStr, buf: &mut BytesMut) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}
