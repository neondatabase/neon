//! Vendoring of serde_json's string escaping code.
//!
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L1514-L1552>
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L2081-L2157>
//! Licensed by David Tolnay under MIT or Apache-2.0.
//!
//! With modifications by Conrad Ludgate on behalf of Neon.

use std::fmt;

use str::{format_escaped_fmt, format_escaped_str};

mod str;

#[must_use]
/// Serialize a single json value.
pub struct ValueSer<'buf> {
    buf: &'buf mut Vec<u8>,
}

impl<'buf> ValueSer<'buf> {
    /// Create a new json value serializer.
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self { buf }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.buf
    }

    /// Write the raw value to the serializer.
    #[inline]
    pub fn raw(self, value: &RawValue) {
        self.buf.extend_from_slice(&value.0);
    }

    /// Write the str to the serializer.
    #[inline]
    pub fn str(self, s: &str) {
        format_escaped_str(self.buf, s);
    }

    /// Write the format string to the serializer.
    #[inline]
    pub fn str_fmt(self, s: fmt::Arguments) {
        if let Some(s) = s.as_str() {
            self.str(s);
        } else {
            format_escaped_fmt(self.buf, s);
        }
    }

    /// Write the integer to the serializer.
    #[inline]
    pub fn int(self, x: impl itoa::Integer) {
        write_int(x, self.buf);
    }

    /// Write the float to the serializer.
    #[inline]
    pub fn float(self, x: impl ryu::Float) {
        write_float(x, self.buf);
    }

    /// Write the bool to the serializer.
    #[inline]
    pub fn bool(self, x: bool) {
        let bool = if x { "true" } else { "false" };
        self.buf.extend_from_slice(bool.as_bytes());
    }

    /// Write null to the serializer.
    #[inline]
    pub fn null(self) {
        self.buf.extend_from_slice(b"null");
    }

    /// Start a new object serializer.
    #[inline]
    pub fn object(self) -> ObjectSer<'buf> {
        ObjectSer::new(self.buf)
    }

    /// Start a new list serializer.
    #[inline]
    pub fn list(self) -> ListSer<'buf> {
        ListSer::new(self.buf)
    }
}

#[must_use]
/// Serialize a json object.
pub struct ObjectSer<'buf> {
    buf: &'buf mut Vec<u8>,
    start: usize,
    prefix: u8,
}

impl<'buf> ObjectSer<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>) -> Self {
        let start = buf.len();
        Self {
            buf,
            start,
            prefix: b'{',
        }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.buf
    }

    /// Start a new object entry with the given string key, returning a [`ValueSer`] for the associated value.
    #[inline]
    pub fn entry(&mut self, key: &str) -> ValueSer {
        self.entry_inner(|b| format_escaped_str(b, key))
    }

    /// Start a new object entry with the given format string key, returning a [`ValueSer`] for the associated value.
    #[inline]
    pub fn entry_fmt(&mut self, key: fmt::Arguments) -> ValueSer {
        if let Some(key) = key.as_str() {
            self.entry(key)
        } else {
            self.entry_inner(|b| format_escaped_fmt(b, key))
        }
    }

    #[inline]
    fn entry_inner(&mut self, f: impl FnOnce(&mut Vec<u8>)) -> ValueSer {
        self.buf.push(self.prefix);
        self.prefix = b',';

        f(self.buf);

        self.buf.push(b':');
        ValueSer { buf: self.buf }
    }

    /// Reset the buffer back to before this object was started.
    #[inline]
    pub fn rollback(self) {
        drop(self);
    }

    /// Finish the object ser.
    #[inline]
    pub fn finish(self) {
        // don't trigger the drop handler which triggers a rollback.
        // this won't cause memory leaks because `ObjectSer` owns no allocations.
        let Self { buf, prefix, .. } = &mut *std::mem::ManuallyDrop::new(self);

        if *prefix == b'{' {
            buf.push(b'{');
        }
        buf.push(b'}');
    }
}

impl Drop for ObjectSer<'_> {
    fn drop(&mut self) {
        self.buf.truncate(self.start);
    }
}

#[must_use]
/// Serialize a json object.
pub struct ListSer<'buf> {
    buf: &'buf mut Vec<u8>,
    start: usize,
    prefix: u8,
}

impl<'buf> ListSer<'buf> {
    #[inline]
    fn new(buf: &'buf mut Vec<u8>) -> Self {
        let start = buf.len();
        Self {
            buf,
            start,
            prefix: b'[',
        }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.buf
    }

    /// Start a new value entry in this list.
    #[inline]
    pub fn entry(&mut self) -> ValueSer {
        self.buf.push(self.prefix);
        self.prefix = b',';
        ValueSer { buf: self.buf }
    }

    /// Reset the buffer back to before this object was started.
    #[inline]
    pub fn rollback(self) {
        drop(self);
    }

    /// Finish the list ser.
    #[inline]
    pub fn finish(self) {
        // don't trigger the drop handler which triggers a rollback.
        // this won't cause memory leaks because `ListSet` owns no allocations.
        let Self { buf, prefix, .. } = &mut *std::mem::ManuallyDrop::new(self);

        if *prefix == b'[' {
            buf.push(b'[');
        }
        buf.push(b']');
    }
}

impl Drop for ListSer<'_> {
    fn drop(&mut self) {
        self.buf.truncate(self.start);
    }
}

/// Represents an already serialized json value.
#[repr(transparent)]
pub struct RawValue([u8]);

impl RawValue {
    /// A constant RawValue with the JSON value `null`.
    pub const NULL: &'static RawValue = RawValue::new_unchecked(b"null");
    /// A constant RawValue with the JSON value `true`.
    pub const TRUE: &'static RawValue = RawValue::new_unchecked(b"true");
    /// A constant RawValue with the JSON value `false`.
    pub const FALSE: &'static RawValue = RawValue::new_unchecked(b"false");

    /// Does no serialization, assumes the given byte-tring is already valid json.
    ///
    /// # Validity
    ///
    /// This does not cause any unsoundness if used incorrectly.
    /// It will only produce incorrect json.
    pub const fn new_unchecked(json: &[u8]) -> &Self {
        // Safety: `RawValue` is `repr(transparent)` over `[u8]`.
        // See <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/raw.rs#L121-L133>
        unsafe { std::mem::transmute::<&[u8], &RawValue>(json) }
    }

    /// Does no serialization, assumes the given byte-string is already valid json.
    ///
    /// # Validity
    ///
    /// This does not cause any unsoundness if used incorrectly.
    /// It will only produce incorrect json.
    pub fn boxed_new_unchecked(json: Box<[u8]>) -> Box<Self> {
        // Safety: `RawValue` is `repr(transparent)` over `[u8]`.
        // See <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/raw.rs#L121-L133>
        unsafe { std::mem::transmute::<Box<[u8]>, Box<RawValue>>(json) }
    }

    #[inline]
    pub fn str(s: &str) -> Box<Self> {
        let mut v = vec![];
        v.reserve_exact(2 + s.len());
        format_escaped_str(&mut v, s);
        Self::boxed_new_unchecked(v.into_boxed_slice())
    }

    #[inline]
    pub fn str_fmt(s: fmt::Arguments) -> Box<Self> {
        if let Some(s) = s.as_str() {
            return Self::str(s);
        }

        let mut v = vec![];
        format_escaped_fmt(&mut v, s);
        Self::boxed_new_unchecked(v.into_boxed_slice())
    }

    #[inline]
    pub fn int(x: impl itoa::Integer) -> Box<Self> {
        Self::boxed_new_unchecked(itoa::Buffer::new().format(x).as_bytes().into())
    }

    #[inline]
    pub fn float(x: impl ryu::Float) -> Box<Self> {
        Self::boxed_new_unchecked(ryu::Buffer::new().format(x).as_bytes().into())
    }

    #[inline]
    pub fn bool(x: bool) -> &'static Self {
        if x { Self::TRUE } else { Self::FALSE }
    }

    #[inline]
    pub fn null() -> &'static Self {
        Self::NULL
    }
}

fn write_int(x: impl itoa::Integer, b: &mut Vec<u8>) {
    b.extend_from_slice(itoa::Buffer::new().format(x).as_bytes());
}

fn write_float(x: impl ryu::Float, b: &mut Vec<u8>) {
    b.extend_from_slice(ryu::Buffer::new().format(x).as_bytes());
}
