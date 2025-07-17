//! A JSON serialization lib, designed for more flexibility than `serde_json` offers.
//!
//! Features:
//!
//! ## Dynamic construction
//!
//! Sometimes you have dynamic values you want to serialize, that are not already in a serde-aware model like a struct or a Vec etc.
//! To achieve this with serde, you need to implement a lot of different traits on a lot of different new-types.
//! Because of this, it's often easier to give-in and pull all the data into a serde-aware model (`serde_json::Value` or some intermediate struct),
//! but that is often not very efficient.
//!
//! This crate allows full control over the JSON encoding without needing to implement any extra traits. Just call the
//! relevant functions, and it will guarantee a correctly encoded JSON value.
//!
//! ## Async construction
//!
//! Similar to the above, sometimes the values arrive asynchronously. Often collecting those values in memory
//! is more expensive than writing them as JSON, since the overheads of `Vec` and `String` is much higher, however
//! there are exceptions.
//!
//! Serializing to JSON all in one go is also more CPU intensive and can cause lag spikes,
//! whereas serializing values incrementally spreads out the CPU load and reduces lag.
//!
//! ## Examples
//!
//! To represent the following JSON as a compact string
//!
//! ```json
//! {
//!   "results": {
//!     "rows": [
//!       {
//!         "id": 1,
//!         "value": null
//!       },
//!       {
//!         "id": 2,
//!         "value": "hello"
//!       }
//!     ]
//!   }
//! }
//! ```
//!
//! We can use the following code:
//!
//! ```
//! // create the outer object
//! let s = json::value_to_string!(|v| json::value_as_object!(|v| {
//!     // create an entry with key "results" and start an object value associated with it.
//!     let results = v.key("results");
//!     json::value_as_object!(|results| {
//!         // create an entry with key "rows" and start an list value associated with it.
//!         let rows = results.key("rows");
//!         json::value_as_list!(|rows| {
//!             // create a list entry and start an object value associated with it.
//!             let row = rows.entry();
//!             json::value_as_object!(|row| {
//!                 // add entry "id": 1
//!                 row.entry("id", 1);
//!                 // add entry "value": null
//!                 row.entry("value", json::Null);
//!             });
//!
//!             // create a list entry and start an object value associated with it.
//!             let row = rows.entry();
//!             json::value_as_object!(|row| {
//!                 // add entry "id": 2
//!                 row.entry("id", 2);
//!                 // add entry "value": "hello"
//!                 row.entry("value", "hello");
//!             });
//!         });
//!     });
//! }));
//!
//! assert_eq!(s, r#"{"results":{"rows":[{"id":1,"value":null},{"id":2,"value":"hello"}]}}"#);
//! ```

mod macros;
mod str;
mod value;

pub use str::EscapedStr;
pub use value::{KeyEncoder, Null, ValueEncoder};

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

    /// Write raw bytes to the buf. This must be already JSON encoded.
    #[inline]
    pub fn write_raw_json(self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        self.finish();
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

#[must_use]
/// Serialize a json object.
pub struct ObjectSer<'buf> {
    value: ValueSer<'buf>,
    start: usize,
}

impl<'buf> ObjectSer<'buf> {
    /// Start a new object serializer.
    #[inline]
    pub fn new(value: ValueSer<'buf>) -> Self {
        value.buf.push(b'{');
        let start = value.buf.len();
        Self { value, start }
    }

    /// Borrow the underlying buffer
    pub fn as_buffer(&self) -> &[u8] {
        self.value.as_buffer()
    }

    /// Start a new object entry with the given string key, returning a [`ValueSer`] for the associated value.
    #[inline]
    pub fn key(&mut self, key: impl KeyEncoder) -> ValueSer<'_> {
        // we create a psuedo value to write the key into.
        let start = self.start;
        self.entry_inner(|buf| key.encode(ValueSer { buf, start }))
    }

    /// Write an entry (key-value pair) to the object.
    #[inline]
    pub fn entry(&mut self, key: impl KeyEncoder, val: impl ValueEncoder) {
        self.key(key).value(val);
    }

    #[inline]
    fn entry_inner(&mut self, f: impl FnOnce(&mut Vec<u8>)) -> ValueSer<'_> {
        // track before the separator so we the value is rolled back it also removes the separator.
        let start = self.value.buf.len();

        // push separator if necessary
        if self.value.buf.len() > self.start {
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
    pub fn rollback(self) -> ValueSer<'buf> {
        // Do not fully reset the value, only reset it to before the `{`.
        // This ensures any `,` before this value are not clobbered.
        self.value.buf.truncate(self.start - 1);
        self.value
    }

    /// Finish the object ser.
    #[inline]
    pub fn finish(self) {
        self.value.buf.push(b'}');
        self.value.finish();
    }
}

#[must_use]
/// Serialize a json object.
pub struct ListSer<'buf> {
    value: ValueSer<'buf>,
    start: usize,
}

impl<'buf> ListSer<'buf> {
    /// Start a new list serializer.
    #[inline]
    pub fn new(value: ValueSer<'buf>) -> Self {
        value.buf.push(b'[');
        let start = value.buf.len();
        Self { value, start }
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
    pub fn entry(&mut self) -> ValueSer<'_> {
        // track before the separator so we the value is rolled back it also removes the separator.
        let start = self.value.buf.len();

        // push separator if necessary
        if self.value.buf.len() > self.start {
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
    pub fn rollback(self) -> ValueSer<'buf> {
        // Do not fully reset the value, only reset it to before the `[`.
        // This ensures any `,` before this value are not clobbered.
        self.value.buf.truncate(self.start - 1);
        self.value
    }

    /// Finish the object ser.
    #[inline]
    pub fn finish(self) {
        self.value.buf.push(b']');
        self.value.finish();
    }
}

#[cfg(test)]
mod tests {
    use crate::{Null, ValueSer};

    #[test]
    fn object() {
        let mut buf = vec![];
        let mut object = ValueSer::new(&mut buf).object();
        object.entry("foo", "bar");
        object.entry("baz", Null);
        object.finish();

        assert_eq!(buf, br#"{"foo":"bar","baz":null}"#);
    }

    #[test]
    fn list() {
        let mut buf = vec![];
        let mut list = ValueSer::new(&mut buf).list();
        list.entry().value("bar");
        list.entry().value(Null);
        list.finish();

        assert_eq!(buf, br#"["bar",null]"#);
    }

    #[test]
    fn object_macro() {
        let res = crate::value_to_string!(|obj| {
            crate::value_as_object!(|obj| {
                obj.entry("foo", "bar");
                obj.entry("baz", Null);
            })
        });

        assert_eq!(res, r#"{"foo":"bar","baz":null}"#);
    }

    #[test]
    fn list_macro() {
        let res = crate::value_to_string!(|list| {
            crate::value_as_list!(|list| {
                list.entry().value("bar");
                list.entry().value(Null);
            })
        });

        assert_eq!(res, r#"["bar",null]"#);
    }

    #[test]
    fn rollback_on_drop() {
        let res = crate::value_to_string!(|list| {
            crate::value_as_list!(|list| {
                list.entry().value("bar");

                'cancel: {
                    let nested_list = list.entry();
                    crate::value_as_list!(|nested_list| {
                        nested_list.entry().value(1);

                        assert_eq!(nested_list.as_buffer(), br#"["bar",[1"#);
                        if true {
                            break 'cancel;
                        }
                    })
                }

                assert_eq!(list.as_buffer(), br#"["bar""#);

                list.entry().value(Null);
            })
        });

        assert_eq!(res, r#"["bar",null]"#);
    }

    #[test]
    fn rollback_object() {
        let res = crate::value_to_string!(|obj| {
            crate::value_as_object!(|obj| {
                let entry = obj.key("1");
                entry.value(1_i32);

                let entry = obj.key("2");
                let entry = {
                    let mut nested_obj = entry.object();
                    nested_obj.entry("foo", "bar");
                    nested_obj.rollback()
                };

                entry.value(2_i32);
            })
        });

        assert_eq!(res, r#"{"1":1,"2":2}"#);
    }

    #[test]
    fn rollback_list() {
        let res = crate::value_to_string!(|list| {
            crate::value_as_list!(|list| {
                let entry = list.entry();
                entry.value(1_i32);

                let entry = list.entry();
                let entry = {
                    let mut nested_list = entry.list();
                    nested_list.push("foo");
                    nested_list.rollback()
                };

                entry.value(2_i32);
            })
        });

        assert_eq!(res, r#"[1,2]"#);
    }

    #[test]
    fn string_escaping() {
        let mut buf = vec![];
        let mut object = ValueSer::new(&mut buf).object();

        let key = "hello";
        let value = "\n world";

        object.entry(format_args!("{key:?}"), value);
        object.finish();

        assert_eq!(buf, br#"{"\"hello\"":"\n world"}"#);
    }
}
