//! Vendoring of serde_json's string escaping code.
//!
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L1514-L1552>
//! <https://github.com/serde-rs/json/blob/c1826ebcccb1a520389c6b78ad3da15db279220d/src/ser.rs#L2081-L2157>
//! Licensed by David Tolnay under MIT or Apache-2.0.
//!
//! With modifications by Conrad Ludgate on behalf of Neon.

use std::{fmt, mem::forget};

use str::{format_escaped_fmt, format_escaped_str};

mod str;

// pub trait KeyEncoder {
//     fn encode<'a>(&self, obj: &'a mut ObjectSer) -> ValueSer<'a>;
// }

// impl KeyEncoder for str {
//     fn encode<'a>(&self, obj: &'a mut ObjectSer) -> ValueSer<'a> {
//         obj.entry(self)
//     }
// }

// impl KeyEncoder for fmt::Arguments<'_> {
//     fn encode<'a>(&self, obj: &'a mut ObjectSer) -> ValueSer<'a> {
//         obj.entry_fmt(*self)
//     }
// }

pub trait ValueEncoder {
    fn encode(&self, v: ValueSer);
}

impl<T: ?Sized + ValueEncoder> ValueEncoder for &T {
    fn encode(&self, v: ValueSer) {
        T::encode(self, v);
    }
}

impl ValueEncoder for str {
    fn encode(&self, v: ValueSer) {
        v.str(self);
    }
}

impl ValueEncoder for fmt::Arguments<'_> {
    fn encode(&self, v: ValueSer) {
        v.str_fmt(*self);
    }
}

macro_rules! int {
    [$($t:ty),*] => {
        $(
            impl ValueEncoder for $t {
                fn encode(&self, v: ValueSer) {
                    v.int(*self);
                }
            }
        )*
    };
}

int![u8, u16, u32, u64, usize, u128];
int![i8, i16, i32, i64, isize, i128];

macro_rules! float {
    [$($t:ty),*] => {
        $(
            impl ValueEncoder for $t {
                fn encode(&self, v: ValueSer) {
                    v.float(*self);
                }
            }
        )*
    };
}

float![f32, f64];

impl ValueEncoder for bool {
    fn encode(&self, v: ValueSer) {
        v.bool(*self);
    }
}

#[macro_export]
macro_rules! to_vec {
    (|$val:ident| $body:expr) => {{
        let mut buf = vec![];
        let $val = $crate::ValueSer::new(&mut buf);
        let _: () = $body;
        buf
    }};
}

#[macro_export]
macro_rules! to_string {
    (|$val:ident| $body:expr) => {{
        ::std::string::String::from_utf8($crate::to_vec!(|$val| $body))
            .expect("json should be valid utf8")
    }};
}

#[macro_export]
macro_rules! object {
    ($val:expr, |$obj:ident| $body:expr) => {{
        let mut obj = $val.object();

        let $obj = &mut obj;
        let res = $body;

        obj.finish();
        res
    }};
}

#[macro_export]
macro_rules! list {
    ($val:expr, |$list:ident| $body:expr) => {{
        let mut list = $val.list();

        let $list = &mut list;
        let res = $body;

        list.finish();
        res
    }};
}

#[macro_export]
macro_rules! json2 {
    ($val:ident = $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => $($tt)*)
    };
}

#[macro_export]
macro_rules! json2string {
    ($($tt:tt)*) => {
        $crate::to_string!(|val| $crate::json2!(val = $($tt)*))
    };
}

#[macro_export]
macro_rules! json2vec {
    ($($tt:tt)*) => {
        $crate::to_vec!(|val| $crate::json2!(val = $($tt)*))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! json_internal {
    // === array parsing ===
    // base case
    (@array[$val:ident] $list:expr => ) => {};
    // comma separator
    (@array[$val:ident] $list:expr => , $($tt:tt)*) => {
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // array val
    (@array[$val:ident] $list:expr => $($tt:tt)*) => {
        let $val: $crate::ValueSer = $list.entry();
        $crate::json_internal!(@val @array[$val] $list => $($tt)*);
    };

    // === object parsing ===
    // base case
    (@obj[$val:ident] $obj:expr => ) => {};
    // comma separator
    (@obj[$val:ident] $obj:expr => , $($tt:tt)*) => {
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // object key
    (@obj[$val:ident] $obj:expr => $key:literal: $($tt:tt)*) => {
        let $val: $crate::ValueSer = $obj.entry($key);
        $crate::json_internal!(@val @obj[$val] $obj => $($tt)*);
    };
    // object fmt key
    (@obj[$val:ident] $obj:expr => [$($fmt:tt)*]: $($tt:tt)*) => {
        let $val: $crate::ValueSer = $obj.entry_fmt(format_args!($($fmt)*));
        $crate::json_internal!(@val @obj[$val] $obj => $($tt)*);
    };

    // === nesting ===

    // nested array
    (@val @obj[$val:ident] $obj:expr => [$($next:tt)*] $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => [$($next)*]);
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // nested object
    (@val @obj[$val:ident] $obj:expr => {$($next:tt)*} $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => {$($next)*});
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // nested null
    (@val @obj[$val:ident] $obj:expr => null $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => null);
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // nested literal
    (@val @obj[$val:ident] $obj:expr => $lit:literal $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => $lit);
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // any valueset expr
    (@val @obj[$val:ident] $obj:expr => |$val2:ident| $next:expr, $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => |$val2| $next);
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // any valueset expr with nothing else trailing
    (@val @obj[$val:ident] $obj:expr => |$val2:ident| $next:expr) => {
        $crate::json_internal!(@val[$val] $val => |$val2| $next);
    };
    // any valueset expr
    (@val @obj[$val:ident] $obj:expr => $next:expr, $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => $next);
        $crate::json_internal!(@obj[$val] $obj => $($tt)*);
    };
    // any valueset expr with nothing else trailing
    (@val @obj[$val:ident] $obj:expr => $next:expr) => {
        $crate::json_internal!(@val[$val] $val => $next);
    };

    // nested array
    (@val @array[$val:ident] $list:expr => [$($next:tt)*] $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => [$($next)*]);
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // nested object
    (@val @array[$val:ident] $list:expr => {$($next:tt)*} $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => {$($next)*});
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // nested null
    (@val @array[$val:ident] $list:expr => null $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => null);
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // nested literal
    (@val @array[$val:ident] $list:expr => $lit:literal $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => $lit);
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // any valueset func
    (@val @array[$val:ident] $list:expr => |$val2:ident| $next:expr, $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => |$val2| $next);
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // any valueencoder expr
    (@val @array[$val:ident] $list:expr => $next:expr, $($tt:tt)*) => {
        $crate::json_internal!(@val[$val] $val => $next);
        $crate::json_internal!(@array[$val] $list => $($tt)*);
    };
    // any valueset func
    (@val @array[$val:ident] $list:expr => |$val2:ident| $next:expr) => {
        $crate::json_internal!(@val[$val] $val => |$val2| $next);
    };
    // any valueencoder expr
    (@val @array[$val:ident] $list:expr => $next:expr) => {
        $crate::json_internal!(@val[$val] $val => $next);
    };

    // === values ===

    (@val[$val:ident] $value:expr => [$($next:tt)*]) => {{
        #[allow(unused_mut)]
        let mut list: $crate::ListSer = $value.list();
        $crate::json_internal!(@array[$val] list => $($next)*);
        list.finish();
    }};
    (@val[$val:ident] $value:expr => {$($next:tt)*}) => {{
        #[allow(unused_mut)]
        let mut object: $crate::ObjectSer = $value.object();
        $crate::json_internal!(@obj[$val] object => $($next)*);
        object.finish();
    }};
    (@val[$val:ident] $value:expr => null) => {
        $value.null();
    };
    (@val[$val:ident] $value:expr => |$val2:ident| $next:expr) => {{
        let $val2: $crate::ValueSer = $value;
        $next
    }};
    (@val[$val:ident] $value:expr => $next:expr) => {
        $crate::ValueEncoder::encode(&$next, $value);
    };
}

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
        if self.prefix == b'{' {
            self.buf.push(b'{');
        }
        self.buf.push(b'}');

        // don't trigger the drop handler which triggers a rollback.
        // this won't cause memory leaks because `ObjectSer` owns no allocations.
        forget(self);
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
        if self.prefix == b'[' {
            self.buf.push(b'[');
        }
        self.buf.push(b']');

        // don't trigger the drop handler which triggers a rollback.
        // this won't cause memory leaks because `ListSer` owns no allocations.
        forget(self);
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

#[cfg(test)]
mod tests {
    use crate::ValueSer;

    #[test]
    fn object() {
        let mut buf = vec![];
        let mut object = ValueSer::new(&mut buf).object();
        object.entry("foo").str("bar");
        object.entry("baz").null();
        object.finish();

        assert_eq!(buf, br#"{"foo":"bar","baz":null}"#);
    }

    #[test]
    fn list() {
        let mut buf = vec![];
        let mut list = ValueSer::new(&mut buf).list();
        list.entry().str("bar");
        list.entry().null();
        list.finish();

        assert_eq!(buf, br#"["bar",null]"#);
    }

    #[test]
    fn object_macro() {
        let res = crate::to_string!(|val| {
            crate::object!(val, |obj| {
                obj.entry("foo").str("bar");
                obj.entry("baz").null();
            })
        });

        assert_eq!(res, r#"{"foo":"bar","baz":null}"#);
    }

    #[test]
    fn list_macro() {
        let res = crate::to_string!(|val| {
            crate::list!(val, |list| {
                list.entry().str("bar");
                list.entry().null();
            })
        });

        assert_eq!(res, r#"["bar",null]"#);
    }

    #[test]
    fn json2macro() {
        let key = "key";

        'demo: {
            let string = crate::json2string!([
                "bar",
                null,
                {
                    "abc": true,
                    ["prefix_{key}"]: 1,
                },
                [[[[[[]]]]]],
                {
                    "foo": |val| {
                        if false {
                            break 'demo;
                        }
                        val.str("foo");
                    }
                }
            ]);

            assert_eq!(
                string,
                r#"["bar",null,{"abc":true,"prefix_key":1},[[[[[[]]]]]],{}]"#
            );
        }
    }
}
