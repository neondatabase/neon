//! Conversions to and from Postgres's binary format for various types.
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use std::boxed::Box as StdBox;
use std::error::Error;
use std::str;

use crate::Oid;

#[cfg(test)]
mod test;

/// Serializes a `TEXT`, `VARCHAR`, `CHAR(n)`, `NAME`, or `CITEXT` value.
#[inline]
pub fn text_to_sql(v: &str, buf: &mut BytesMut) {
    buf.put_slice(v.as_bytes());
}

/// Deserializes a `TEXT`, `VARCHAR`, `CHAR(n)`, `NAME`, or `CITEXT` value.
#[inline]
pub fn text_from_sql(buf: &[u8]) -> Result<&str, StdBox<dyn Error + Sync + Send>> {
    Ok(str::from_utf8(buf)?)
}

/// Deserializes a `"char"` value.
#[inline]
pub fn char_from_sql(mut buf: &[u8]) -> Result<i8, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i8()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `OID` value.
#[inline]
pub fn oid_to_sql(v: Oid, buf: &mut BytesMut) {
    buf.put_u32(v);
}

/// Deserializes an `OID` value.
#[inline]
pub fn oid_from_sql(mut buf: &[u8]) -> Result<Oid, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_u32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// A fallible iterator over `HSTORE` entries.
pub struct HstoreEntries<'a> {
    remaining: i32,
    buf: &'a [u8],
}

impl<'a> FallibleIterator for HstoreEntries<'a> {
    type Item = (&'a str, Option<&'a str>);
    type Error = StdBox<dyn Error + Sync + Send>;

    #[inline]
    #[allow(clippy::type_complexity)]
    fn next(
        &mut self,
    ) -> Result<Option<(&'a str, Option<&'a str>)>, StdBox<dyn Error + Sync + Send>> {
        if self.remaining == 0 {
            if !self.buf.is_empty() {
                return Err("invalid buffer size".into());
            }
            return Ok(None);
        }

        self.remaining -= 1;

        let key_len = self.buf.read_i32::<BigEndian>()?;
        if key_len < 0 {
            return Err("invalid key length".into());
        }
        let (key, buf) = self.buf.split_at(key_len as usize);
        let key = str::from_utf8(key)?;
        self.buf = buf;

        let value_len = self.buf.read_i32::<BigEndian>()?;
        let value = if value_len < 0 {
            None
        } else {
            let (value, buf) = self.buf.split_at(value_len as usize);
            let value = str::from_utf8(value)?;
            self.buf = buf;
            Some(value)
        };

        Ok(Some((key, value)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

/// Deserializes an array value.
#[inline]
pub fn array_from_sql(mut buf: &[u8]) -> Result<Array<'_>, StdBox<dyn Error + Sync + Send>> {
    let dimensions = buf.read_i32::<BigEndian>()?;
    if dimensions < 0 {
        return Err("invalid dimension count".into());
    }

    let mut r = buf;
    let mut elements = 1i32;
    for _ in 0..dimensions {
        let len = r.read_i32::<BigEndian>()?;
        if len < 0 {
            return Err("invalid dimension size".into());
        }
        let _lower_bound = r.read_i32::<BigEndian>()?;
        elements = match elements.checked_mul(len) {
            Some(elements) => elements,
            None => return Err("too many array elements".into()),
        };
    }

    if dimensions == 0 {
        elements = 0;
    }

    Ok(Array {
        dimensions,
        elements,
        buf,
    })
}

/// A Postgres array.
pub struct Array<'a> {
    dimensions: i32,
    elements: i32,
    buf: &'a [u8],
}

impl<'a> Array<'a> {
    /// Returns an iterator over the dimensions of the array.
    #[inline]
    pub fn dimensions(&self) -> ArrayDimensions<'a> {
        ArrayDimensions(&self.buf[..self.dimensions as usize * 8])
    }

    /// Returns an iterator over the values of the array.
    #[inline]
    pub fn values(&self) -> ArrayValues<'a> {
        ArrayValues {
            remaining: self.elements,
            buf: &self.buf[self.dimensions as usize * 8..],
        }
    }
}

/// An iterator over the dimensions of an array.
pub struct ArrayDimensions<'a>(&'a [u8]);

impl FallibleIterator for ArrayDimensions<'_> {
    type Item = ArrayDimension;
    type Error = StdBox<dyn Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<ArrayDimension>, StdBox<dyn Error + Sync + Send>> {
        if self.0.is_empty() {
            return Ok(None);
        }

        let len = self.0.read_i32::<BigEndian>()?;
        let lower_bound = self.0.read_i32::<BigEndian>()?;

        Ok(Some(ArrayDimension { len, lower_bound }))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len() / 8;
        (len, Some(len))
    }
}

/// Information about a dimension of an array.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ArrayDimension {
    /// The length of this dimension.
    pub len: i32,

    /// The base value used to index into this dimension.
    pub lower_bound: i32,
}

/// An iterator over the values of an array, in row-major order.
pub struct ArrayValues<'a> {
    remaining: i32,
    buf: &'a [u8],
}

impl<'a> FallibleIterator for ArrayValues<'a> {
    type Item = Option<&'a [u8]>;
    type Error = StdBox<dyn Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<Option<&'a [u8]>>, StdBox<dyn Error + Sync + Send>> {
        if self.remaining == 0 {
            if !self.buf.is_empty() {
                return Err("invalid message length: arrayvalue not drained".into());
            }
            return Ok(None);
        }
        self.remaining -= 1;

        let len = self.buf.read_i32::<BigEndian>()?;
        let val = if len < 0 {
            None
        } else {
            if self.buf.len() < len as usize {
                return Err("invalid value length".into());
            }

            let (val, buf) = self.buf.split_at(len as usize);
            self.buf = buf;
            Some(val)
        };

        Ok(Some(val))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

/// Serializes a Postgres ltree string
#[inline]
pub fn ltree_to_sql(v: &str, buf: &mut BytesMut) {
    // A version number is prepended to an ltree string per spec
    buf.put_u8(1);
    // Append the rest of the query
    buf.put_slice(v.as_bytes());
}

/// Deserialize a Postgres ltree string
#[inline]
pub fn ltree_from_sql(buf: &[u8]) -> Result<&str, StdBox<dyn Error + Sync + Send>> {
    match buf {
        // Remove the version number from the front of the ltree per spec
        [1u8, rest @ ..] => Ok(str::from_utf8(rest)?),
        _ => Err("ltree version 1 only supported".into()),
    }
}

/// Serializes a Postgres lquery string
#[inline]
pub fn lquery_to_sql(v: &str, buf: &mut BytesMut) {
    // A version number is prepended to an lquery string per spec
    buf.put_u8(1);
    // Append the rest of the query
    buf.put_slice(v.as_bytes());
}

/// Deserialize a Postgres lquery string
#[inline]
pub fn lquery_from_sql(buf: &[u8]) -> Result<&str, StdBox<dyn Error + Sync + Send>> {
    match buf {
        // Remove the version number from the front of the lquery per spec
        [1u8, rest @ ..] => Ok(str::from_utf8(rest)?),
        _ => Err("lquery version 1 only supported".into()),
    }
}

/// Serializes a Postgres ltxtquery string
#[inline]
pub fn ltxtquery_to_sql(v: &str, buf: &mut BytesMut) {
    // A version number is prepended to an ltxtquery string per spec
    buf.put_u8(1);
    // Append the rest of the query
    buf.put_slice(v.as_bytes());
}

/// Deserialize a Postgres ltxtquery string
#[inline]
pub fn ltxtquery_from_sql(buf: &[u8]) -> Result<&str, StdBox<dyn Error + Sync + Send>> {
    match buf {
        // Remove the version number from the front of the ltxtquery per spec
        [1u8, rest @ ..] => Ok(str::from_utf8(rest)?),
        _ => Err("ltxtquery version 1 only supported".into()),
    }
}
