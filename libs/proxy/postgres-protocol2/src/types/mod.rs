//! Conversions to and from Postgres's binary format for various types.
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use std::boxed::Box as StdBox;
use std::error::Error;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str;

use crate::{write_nullable, FromUsize, IsNull, Lsn, Oid};

#[cfg(test)]
mod test;

const RANGE_UPPER_UNBOUNDED: u8 = 0b0001_0000;
const RANGE_LOWER_UNBOUNDED: u8 = 0b0000_1000;
const RANGE_UPPER_INCLUSIVE: u8 = 0b0000_0100;
const RANGE_LOWER_INCLUSIVE: u8 = 0b0000_0010;
const RANGE_EMPTY: u8 = 0b0000_0001;

const PGSQL_AF_INET: u8 = 2;
const PGSQL_AF_INET6: u8 = 3;

/// Serializes a `BOOL` value.
#[inline]
pub fn bool_to_sql(v: bool, buf: &mut BytesMut) {
    buf.put_u8(v as u8);
}

/// Deserializes a `BOOL` value.
#[inline]
pub fn bool_from_sql(buf: &[u8]) -> Result<bool, StdBox<dyn Error + Sync + Send>> {
    if buf.len() != 1 {
        return Err("invalid buffer size".into());
    }

    Ok(buf[0] != 0)
}

/// Serializes a `BYTEA` value.
#[inline]
pub fn bytea_to_sql(v: &[u8], buf: &mut BytesMut) {
    buf.put_slice(v);
}

/// Deserializes a `BYTEA value.
#[inline]
pub fn bytea_from_sql(buf: &[u8]) -> &[u8] {
    buf
}

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

/// Serializes a `"char"` value.
#[inline]
pub fn char_to_sql(v: i8, buf: &mut BytesMut) {
    buf.put_i8(v);
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

/// Serializes an `INT2` value.
#[inline]
pub fn int2_to_sql(v: i16, buf: &mut BytesMut) {
    buf.put_i16(v);
}

/// Deserializes an `INT2` value.
#[inline]
pub fn int2_from_sql(mut buf: &[u8]) -> Result<i16, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i16::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `INT4` value.
#[inline]
pub fn int4_to_sql(v: i32, buf: &mut BytesMut) {
    buf.put_i32(v);
}

/// Deserializes an `INT4` value.
#[inline]
pub fn int4_from_sql(mut buf: &[u8]) -> Result<i32, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i32::<BigEndian>()?;
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

/// Serializes an `INT8` value.
#[inline]
pub fn int8_to_sql(v: i64, buf: &mut BytesMut) {
    buf.put_i64(v);
}

/// Deserializes an `INT8` value.
#[inline]
pub fn int8_from_sql(mut buf: &[u8]) -> Result<i64, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes a `PG_LSN` value.
#[inline]
pub fn lsn_to_sql(v: Lsn, buf: &mut BytesMut) {
    buf.put_u64(v);
}

/// Deserializes a `PG_LSN` value.
#[inline]
pub fn lsn_from_sql(mut buf: &[u8]) -> Result<Lsn, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_u64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes a `FLOAT4` value.
#[inline]
pub fn float4_to_sql(v: f32, buf: &mut BytesMut) {
    buf.put_f32(v);
}

/// Deserializes a `FLOAT4` value.
#[inline]
pub fn float4_from_sql(mut buf: &[u8]) -> Result<f32, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_f32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes a `FLOAT8` value.
#[inline]
pub fn float8_to_sql(v: f64, buf: &mut BytesMut) {
    buf.put_f64(v);
}

/// Deserializes a `FLOAT8` value.
#[inline]
pub fn float8_from_sql(mut buf: &[u8]) -> Result<f64, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_f64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(v)
}

/// Serializes an `HSTORE` value.
#[inline]
pub fn hstore_to_sql<'a, I>(
    values: I,
    buf: &mut BytesMut,
) -> Result<(), StdBox<dyn Error + Sync + Send>>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    let base = buf.len();
    buf.put_i32(0);

    let mut count = 0;
    for (key, value) in values {
        count += 1;

        write_pascal_string(key, buf)?;

        match value {
            Some(value) => {
                write_pascal_string(value, buf)?;
            }
            None => buf.put_i32(-1),
        }
    }

    let count = i32::from_usize(count)?;
    BigEndian::write_i32(&mut buf[base..], count);

    Ok(())
}

fn write_pascal_string(s: &str, buf: &mut BytesMut) -> Result<(), StdBox<dyn Error + Sync + Send>> {
    let size = i32::from_usize(s.len())?;
    buf.put_i32(size);
    buf.put_slice(s.as_bytes());
    Ok(())
}

/// Deserializes an `HSTORE` value.
#[inline]
pub fn hstore_from_sql(
    mut buf: &[u8],
) -> Result<HstoreEntries<'_>, StdBox<dyn Error + Sync + Send>> {
    let count = buf.read_i32::<BigEndian>()?;
    if count < 0 {
        return Err("invalid entry count".into());
    }

    Ok(HstoreEntries {
        remaining: count,
        buf,
    })
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

/// Serializes a `VARBIT` or `BIT` value.
#[inline]
pub fn varbit_to_sql<I>(
    len: usize,
    v: I,
    buf: &mut BytesMut,
) -> Result<(), StdBox<dyn Error + Sync + Send>>
where
    I: Iterator<Item = u8>,
{
    let len = i32::from_usize(len)?;
    buf.put_i32(len);

    for byte in v {
        buf.put_u8(byte);
    }

    Ok(())
}

/// Deserializes a `VARBIT` or `BIT` value.
#[inline]
pub fn varbit_from_sql(mut buf: &[u8]) -> Result<Varbit<'_>, StdBox<dyn Error + Sync + Send>> {
    let len = buf.read_i32::<BigEndian>()?;
    if len < 0 {
        return Err("invalid varbit length: varbit < 0".into());
    }
    let bytes = (len as usize + 7) / 8;
    if buf.len() != bytes {
        return Err("invalid message length: varbit mismatch".into());
    }

    Ok(Varbit {
        len: len as usize,
        bytes: buf,
    })
}

/// A `VARBIT` value.
pub struct Varbit<'a> {
    len: usize,
    bytes: &'a [u8],
}

impl<'a> Varbit<'a> {
    /// Returns the number of bits.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Determines if the value has no bits.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the bits as a slice of bytes.
    #[inline]
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// Serializes a `TIMESTAMP` or `TIMESTAMPTZ` value.
///
/// The value should represent the number of microseconds since midnight, January 1st, 2000.
#[inline]
pub fn timestamp_to_sql(v: i64, buf: &mut BytesMut) {
    buf.put_i64(v);
}

/// Deserializes a `TIMESTAMP` or `TIMESTAMPTZ` value.
///
/// The value represents the number of microseconds since midnight, January 1st, 2000.
#[inline]
pub fn timestamp_from_sql(mut buf: &[u8]) -> Result<i64, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid message length: timestamp not drained".into());
    }
    Ok(v)
}

/// Serializes a `DATE` value.
///
/// The value should represent the number of days since January 1st, 2000.
#[inline]
pub fn date_to_sql(v: i32, buf: &mut BytesMut) {
    buf.put_i32(v);
}

/// Deserializes a `DATE` value.
///
/// The value represents the number of days since January 1st, 2000.
#[inline]
pub fn date_from_sql(mut buf: &[u8]) -> Result<i32, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i32::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid message length: date not drained".into());
    }
    Ok(v)
}

/// Serializes a `TIME` or `TIMETZ` value.
///
/// The value should represent the number of microseconds since midnight.
#[inline]
pub fn time_to_sql(v: i64, buf: &mut BytesMut) {
    buf.put_i64(v);
}

/// Deserializes a `TIME` or `TIMETZ` value.
///
/// The value represents the number of microseconds since midnight.
#[inline]
pub fn time_from_sql(mut buf: &[u8]) -> Result<i64, StdBox<dyn Error + Sync + Send>> {
    let v = buf.read_i64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid message length: time not drained".into());
    }
    Ok(v)
}

/// Serializes a `MACADDR` value.
#[inline]
pub fn macaddr_to_sql(v: [u8; 6], buf: &mut BytesMut) {
    buf.put_slice(&v);
}

/// Deserializes a `MACADDR` value.
#[inline]
pub fn macaddr_from_sql(buf: &[u8]) -> Result<[u8; 6], StdBox<dyn Error + Sync + Send>> {
    if buf.len() != 6 {
        return Err("invalid message length: macaddr length mismatch".into());
    }
    let mut out = [0; 6];
    out.copy_from_slice(buf);
    Ok(out)
}

/// Serializes a `UUID` value.
#[inline]
pub fn uuid_to_sql(v: [u8; 16], buf: &mut BytesMut) {
    buf.put_slice(&v);
}

/// Deserializes a `UUID` value.
#[inline]
pub fn uuid_from_sql(buf: &[u8]) -> Result<[u8; 16], StdBox<dyn Error + Sync + Send>> {
    if buf.len() != 16 {
        return Err("invalid message length: uuid size mismatch".into());
    }
    let mut out = [0; 16];
    out.copy_from_slice(buf);
    Ok(out)
}

/// Serializes an array value.
#[inline]
pub fn array_to_sql<T, I, J, F>(
    dimensions: I,
    element_type: Oid,
    elements: J,
    mut serializer: F,
    buf: &mut BytesMut,
) -> Result<(), StdBox<dyn Error + Sync + Send>>
where
    I: IntoIterator<Item = ArrayDimension>,
    J: IntoIterator<Item = T>,
    F: FnMut(T, &mut BytesMut) -> Result<IsNull, StdBox<dyn Error + Sync + Send>>,
{
    let dimensions_idx = buf.len();
    buf.put_i32(0);
    let flags_idx = buf.len();
    buf.put_i32(0);
    buf.put_u32(element_type);

    let mut num_dimensions = 0;
    for dimension in dimensions {
        num_dimensions += 1;
        buf.put_i32(dimension.len);
        buf.put_i32(dimension.lower_bound);
    }

    let num_dimensions = i32::from_usize(num_dimensions)?;
    BigEndian::write_i32(&mut buf[dimensions_idx..], num_dimensions);

    let mut has_nulls = false;
    for element in elements {
        write_nullable(
            |buf| {
                let r = serializer(element, buf);
                if let Ok(IsNull::Yes) = r {
                    has_nulls = true;
                }
                r
            },
            buf,
        )?;
    }

    BigEndian::write_i32(&mut buf[flags_idx..], has_nulls as i32);

    Ok(())
}

/// Deserializes an array value.
#[inline]
pub fn array_from_sql(mut buf: &[u8]) -> Result<Array<'_>, StdBox<dyn Error + Sync + Send>> {
    let dimensions = buf.read_i32::<BigEndian>()?;
    if dimensions < 0 {
        return Err("invalid dimension count".into());
    }
    let has_nulls = buf.read_i32::<BigEndian>()? != 0;
    let element_type = buf.read_u32::<BigEndian>()?;

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
        has_nulls,
        element_type,
        elements,
        buf,
    })
}

/// A Postgres array.
pub struct Array<'a> {
    dimensions: i32,
    has_nulls: bool,
    element_type: Oid,
    elements: i32,
    buf: &'a [u8],
}

impl<'a> Array<'a> {
    /// Returns true if there are `NULL` elements.
    #[inline]
    pub fn has_nulls(&self) -> bool {
        self.has_nulls
    }

    /// Returns the OID of the elements of the array.
    #[inline]
    pub fn element_type(&self) -> Oid {
        self.element_type
    }

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

impl<'a> FallibleIterator for ArrayDimensions<'a> {
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

/// Serializes an empty range.
#[inline]
pub fn empty_range_to_sql(buf: &mut BytesMut) {
    buf.put_u8(RANGE_EMPTY);
}

/// Serializes a range value.
pub fn range_to_sql<F, G>(
    lower: F,
    upper: G,
    buf: &mut BytesMut,
) -> Result<(), StdBox<dyn Error + Sync + Send>>
where
    F: FnOnce(&mut BytesMut) -> Result<RangeBound<IsNull>, StdBox<dyn Error + Sync + Send>>,
    G: FnOnce(&mut BytesMut) -> Result<RangeBound<IsNull>, StdBox<dyn Error + Sync + Send>>,
{
    let tag_idx = buf.len();
    buf.put_u8(0);
    let mut tag = 0;

    match write_bound(lower, buf)? {
        RangeBound::Inclusive(()) => tag |= RANGE_LOWER_INCLUSIVE,
        RangeBound::Exclusive(()) => {}
        RangeBound::Unbounded => tag |= RANGE_LOWER_UNBOUNDED,
    }

    match write_bound(upper, buf)? {
        RangeBound::Inclusive(()) => tag |= RANGE_UPPER_INCLUSIVE,
        RangeBound::Exclusive(()) => {}
        RangeBound::Unbounded => tag |= RANGE_UPPER_UNBOUNDED,
    }

    buf[tag_idx] = tag;

    Ok(())
}

fn write_bound<F>(
    bound: F,
    buf: &mut BytesMut,
) -> Result<RangeBound<()>, StdBox<dyn Error + Sync + Send>>
where
    F: FnOnce(&mut BytesMut) -> Result<RangeBound<IsNull>, StdBox<dyn Error + Sync + Send>>,
{
    let base = buf.len();
    buf.put_i32(0);

    let (null, ret) = match bound(buf)? {
        RangeBound::Inclusive(null) => (Some(null), RangeBound::Inclusive(())),
        RangeBound::Exclusive(null) => (Some(null), RangeBound::Exclusive(())),
        RangeBound::Unbounded => (None, RangeBound::Unbounded),
    };

    match null {
        Some(null) => {
            let len = match null {
                IsNull::No => i32::from_usize(buf.len() - base - 4)?,
                IsNull::Yes => -1,
            };
            BigEndian::write_i32(&mut buf[base..], len);
        }
        None => buf.truncate(base),
    }

    Ok(ret)
}

/// One side of a range.
pub enum RangeBound<T> {
    /// An inclusive bound.
    Inclusive(T),
    /// An exclusive bound.
    Exclusive(T),
    /// No bound.
    Unbounded,
}

/// Deserializes a range value.
#[inline]
pub fn range_from_sql(mut buf: &[u8]) -> Result<Range<'_>, StdBox<dyn Error + Sync + Send>> {
    let tag = buf.read_u8()?;

    if tag == RANGE_EMPTY {
        if !buf.is_empty() {
            return Err("invalid message size".into());
        }
        return Ok(Range::Empty);
    }

    let lower = read_bound(&mut buf, tag, RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE)?;
    let upper = read_bound(&mut buf, tag, RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE)?;

    if !buf.is_empty() {
        return Err("invalid message size".into());
    }

    Ok(Range::Nonempty(lower, upper))
}

#[inline]
fn read_bound<'a>(
    buf: &mut &'a [u8],
    tag: u8,
    unbounded: u8,
    inclusive: u8,
) -> Result<RangeBound<Option<&'a [u8]>>, StdBox<dyn Error + Sync + Send>> {
    if tag & unbounded != 0 {
        Ok(RangeBound::Unbounded)
    } else {
        let len = buf.read_i32::<BigEndian>()?;
        let value = if len < 0 {
            None
        } else {
            let len = len as usize;
            if buf.len() < len {
                return Err("invalid message size".into());
            }
            let (value, tail) = buf.split_at(len);
            *buf = tail;
            Some(value)
        };

        if tag & inclusive != 0 {
            Ok(RangeBound::Inclusive(value))
        } else {
            Ok(RangeBound::Exclusive(value))
        }
    }
}

/// A Postgres range.
pub enum Range<'a> {
    /// An empty range.
    Empty,
    /// A nonempty range.
    Nonempty(RangeBound<Option<&'a [u8]>>, RangeBound<Option<&'a [u8]>>),
}

/// Serializes a point value.
#[inline]
pub fn point_to_sql(x: f64, y: f64, buf: &mut BytesMut) {
    buf.put_f64(x);
    buf.put_f64(y);
}

/// Deserializes a point value.
#[inline]
pub fn point_from_sql(mut buf: &[u8]) -> Result<Point, StdBox<dyn Error + Sync + Send>> {
    let x = buf.read_f64::<BigEndian>()?;
    let y = buf.read_f64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(Point { x, y })
}

/// A Postgres point.
#[derive(Copy, Clone)]
pub struct Point {
    x: f64,
    y: f64,
}

impl Point {
    /// Returns the x coordinate of the point.
    #[inline]
    pub fn x(&self) -> f64 {
        self.x
    }

    /// Returns the y coordinate of the point.
    #[inline]
    pub fn y(&self) -> f64 {
        self.y
    }
}

/// Serializes a box value.
#[inline]
pub fn box_to_sql(x1: f64, y1: f64, x2: f64, y2: f64, buf: &mut BytesMut) {
    buf.put_f64(x1);
    buf.put_f64(y1);
    buf.put_f64(x2);
    buf.put_f64(y2);
}

/// Deserializes a box value.
#[inline]
pub fn box_from_sql(mut buf: &[u8]) -> Result<Box, StdBox<dyn Error + Sync + Send>> {
    let x1 = buf.read_f64::<BigEndian>()?;
    let y1 = buf.read_f64::<BigEndian>()?;
    let x2 = buf.read_f64::<BigEndian>()?;
    let y2 = buf.read_f64::<BigEndian>()?;
    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }
    Ok(Box {
        upper_right: Point { x: x1, y: y1 },
        lower_left: Point { x: x2, y: y2 },
    })
}

/// A Postgres box.
#[derive(Copy, Clone)]
pub struct Box {
    upper_right: Point,
    lower_left: Point,
}

impl Box {
    /// Returns the upper right corner of the box.
    #[inline]
    pub fn upper_right(&self) -> Point {
        self.upper_right
    }

    /// Returns the lower left corner of the box.
    #[inline]
    pub fn lower_left(&self) -> Point {
        self.lower_left
    }
}

/// Serializes a Postgres path.
#[inline]
pub fn path_to_sql<I>(
    closed: bool,
    points: I,
    buf: &mut BytesMut,
) -> Result<(), StdBox<dyn Error + Sync + Send>>
where
    I: IntoIterator<Item = (f64, f64)>,
{
    buf.put_u8(closed as u8);
    let points_idx = buf.len();
    buf.put_i32(0);

    let mut num_points = 0;
    for (x, y) in points {
        num_points += 1;
        buf.put_f64(x);
        buf.put_f64(y);
    }

    let num_points = i32::from_usize(num_points)?;
    BigEndian::write_i32(&mut buf[points_idx..], num_points);

    Ok(())
}

/// Deserializes a Postgres path.
#[inline]
pub fn path_from_sql(mut buf: &[u8]) -> Result<Path<'_>, StdBox<dyn Error + Sync + Send>> {
    let closed = buf.read_u8()? != 0;
    let points = buf.read_i32::<BigEndian>()?;

    Ok(Path {
        closed,
        points,
        buf,
    })
}

/// A Postgres point.
pub struct Path<'a> {
    closed: bool,
    points: i32,
    buf: &'a [u8],
}

impl<'a> Path<'a> {
    /// Determines if the path is closed or open.
    #[inline]
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Returns an iterator over the points in the path.
    #[inline]
    pub fn points(&self) -> PathPoints<'a> {
        PathPoints {
            remaining: self.points,
            buf: self.buf,
        }
    }
}

/// An iterator over the points of a Postgres path.
pub struct PathPoints<'a> {
    remaining: i32,
    buf: &'a [u8],
}

impl<'a> FallibleIterator for PathPoints<'a> {
    type Item = Point;
    type Error = StdBox<dyn Error + Sync + Send>;

    #[inline]
    fn next(&mut self) -> Result<Option<Point>, StdBox<dyn Error + Sync + Send>> {
        if self.remaining == 0 {
            if !self.buf.is_empty() {
                return Err("invalid message length: path points not drained".into());
            }
            return Ok(None);
        }
        self.remaining -= 1;

        let x = self.buf.read_f64::<BigEndian>()?;
        let y = self.buf.read_f64::<BigEndian>()?;

        Ok(Some(Point { x, y }))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

/// Serializes a Postgres inet.
#[inline]
pub fn inet_to_sql(addr: IpAddr, netmask: u8, buf: &mut BytesMut) {
    let family = match addr {
        IpAddr::V4(_) => PGSQL_AF_INET,
        IpAddr::V6(_) => PGSQL_AF_INET6,
    };
    buf.put_u8(family);
    buf.put_u8(netmask);
    buf.put_u8(0); // is_cidr
    match addr {
        IpAddr::V4(addr) => {
            buf.put_u8(4);
            buf.put_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
            buf.put_u8(16);
            buf.put_slice(&addr.octets());
        }
    }
}

/// Deserializes a Postgres inet.
#[inline]
pub fn inet_from_sql(mut buf: &[u8]) -> Result<Inet, StdBox<dyn Error + Sync + Send>> {
    let family = buf.read_u8()?;
    let netmask = buf.read_u8()?;
    buf.read_u8()?; // is_cidr
    let len = buf.read_u8()?;

    let addr = match family {
        PGSQL_AF_INET => {
            if netmask > 32 {
                return Err("invalid IPv4 netmask".into());
            }
            if len != 4 {
                return Err("invalid IPv4 address length".into());
            }
            let mut addr = [0; 4];
            buf.read_exact(&mut addr)?;
            IpAddr::V4(Ipv4Addr::from(addr))
        }
        PGSQL_AF_INET6 => {
            if netmask > 128 {
                return Err("invalid IPv6 netmask".into());
            }
            if len != 16 {
                return Err("invalid IPv6 address length".into());
            }
            let mut addr = [0; 16];
            buf.read_exact(&mut addr)?;
            IpAddr::V6(Ipv6Addr::from(addr))
        }
        _ => return Err("invalid IP family".into()),
    };

    if !buf.is_empty() {
        return Err("invalid buffer size".into());
    }

    Ok(Inet { addr, netmask })
}

/// A Postgres network address.
pub struct Inet {
    addr: IpAddr,
    netmask: u8,
}

impl Inet {
    /// Returns the IP address.
    #[inline]
    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    /// Returns the netmask.
    #[inline]
    pub fn netmask(&self) -> u8 {
        self.netmask
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
