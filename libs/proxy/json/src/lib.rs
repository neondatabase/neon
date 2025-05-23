//! Vendoring of serde_json's string escaping code.
//!
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L1514-L1552>
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L2081-L2157>
//! Licensed by David Tolnay under MIT or Apache-2.0.
//!
//! With modifications by Conrad Ludgate on behalf of Neon.

mod macros;
mod raw;
mod str;

pub use raw::RawValue;

#[must_use]
/// Serialize a single json value.
pub struct ValueSer<'buf> {
    buf: &'buf mut Vec<u8>,
    start: usize,
}

impl<'buf> ValueSer<'buf> {
    /// Create a new json value serializer.
    pub fn new(buf: &'buf mut Vec<u8>) -> Self {
        Self { buf, start: 0 }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.buf
    }

    #[inline]
    pub fn value(self, e: impl ValueEncoder) {
        e.encode(self);
    }

    /// Start a new object serializer.
    #[inline]
    pub fn object(self) -> ObjectSer<'buf> {
        ObjectSer::new(self)
    }

    /// Start a new list serializer.
    #[inline]
    pub fn list(self) -> ListSer<'buf> {
        ListSer::new(self)
    }

    /// Finish the value ser.
    #[inline]
    fn finish(self) {
        // don't trigger the drop handler which triggers a rollback.
        // this won't cause memory leaks because `ValueSet` owns no allocations.
        std::mem::forget(self);
    }
}

impl Drop for ValueSer<'_> {
    fn drop(&mut self) {
        self.buf.truncate(self.start);
    }
}

/// Write a value to the underlying json representation.
pub trait ValueEncoder {
    fn encode(self, v: ValueSer);
}

#[must_use]
/// Serialize a json object.
pub struct ObjectSer<'buf> {
    value: ValueSer<'buf>,
}

impl<'buf> ObjectSer<'buf> {
    /// Start a new object serializer.
    #[inline]
    pub fn new(value: ValueSer<'buf>) -> Self {
        value.buf.push(b'{');
        Self { value }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.value.as_buffer()
    }

    /// Start a new object entry with the given string key, returning a [`ValueSer`] for the associated value.
    #[inline]
    pub fn key(&mut self, key: impl KeyEncoder) -> ValueSer {
        key.write_key(self)
    }

    /// Write an entry (key-value pair) to the object.
    #[inline]
    pub fn entry(&mut self, key: impl KeyEncoder, val: impl ValueEncoder) {
        self.key(key).value(val);
    }

    #[inline]
    fn entry_inner(&mut self, f: impl FnOnce(&mut Vec<u8>)) -> ValueSer {
        let start = self.value.buf.len();

        // push separator if necessary
        if self.value.buf.len() > self.value.start + 1 {
            self.value.buf.push(b',');
        }
        // push key
        f(self.value.buf);
        // push value separator
        self.value.buf.push(b':');

        // return value writer.
        ValueSer {
            buf: self.value.buf,
            start,
        }
    }

    /// Reset the buffer back to before this object was started.
    #[inline]
    pub fn rollback(self) {}

    /// Finish the object ser.
    #[inline]
    pub fn finish(self) {
        self.value.buf.push(b'}');
        self.value.finish();
    }
}

pub trait KeyEncoder {
    fn write_key<'a>(self, obj: &'a mut ObjectSer) -> ValueSer<'a>;
}

#[must_use]
/// Serialize a json object.
pub struct ListSer<'buf> {
    value: ValueSer<'buf>,
}

impl<'buf> ListSer<'buf> {
    /// Start a new list serializer.
    #[inline]
    pub fn new(value: ValueSer<'buf>) -> Self {
        value.buf.push(b'[');
        Self { value }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.value.as_buffer()
    }

    /// Write an value to the list.
    #[inline]
    pub fn push(&mut self, val: impl ValueEncoder) {
        self.entry().value(val);
    }

    /// Start a new value entry in this list.
    #[inline]
    pub fn entry(&mut self) -> ValueSer {
        let start = self.value.buf.len();

        // push separator if necessary
        if self.value.buf.len() > self.value.start + 1 {
            self.value.buf.push(b',');
        }

        // return value writer.
        ValueSer {
            buf: self.value.buf,
            start,
        }
    }

    /// Reset the buffer back to before this object was started.
    #[inline]
    pub fn rollback(self) {}

    /// Finish the object ser.
    #[inline]
    pub fn finish(self) {
        self.value.buf.push(b']');
        self.value.finish();
    }
}

#[cfg(test)]
mod tests {
    use crate::ValueSer;

    #[test]
    fn object() {
        let mut buf = vec![];
        let mut object = ValueSer::new(&mut buf).object();
        object.entry("foo", "bar");
        object.entry("baz", crate::RawValue::NULL);
        object.finish();

        assert_eq!(buf, br#"{"foo":"bar","baz":null}"#);
    }

    #[test]
    fn list() {
        let mut buf = vec![];
        let mut list = ValueSer::new(&mut buf).list();
        list.entry().value("bar");
        list.entry().value(crate::RawValue::NULL);
        list.finish();

        assert_eq!(buf, br#"["bar",null]"#);
    }

    #[test]
    fn object_macro() {
        let res = crate::value_to_string!(|obj| {
            crate::value_as_object!(|obj| {
                obj.entry("foo", "bar");
                obj.entry("baz", crate::RawValue::NULL);
            })
        });

        assert_eq!(res, r#"{"foo":"bar","baz":null}"#);
    }

    #[test]
    fn list_macro() {
        let res = crate::value_to_string!(|list| {
            crate::value_as_list!(|list| {
                list.entry().value("bar");
                list.entry().value(crate::RawValue::NULL);
            })
        });

        assert_eq!(res, r#"["bar",null]"#);
    }
}
