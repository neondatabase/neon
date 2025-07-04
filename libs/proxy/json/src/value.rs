use core::fmt;
use std::collections::{BTreeMap, HashMap};

use crate::str::{format_escaped_fmt, format_escaped_str};
use crate::{KeyEncoder, ObjectSer, ValueSer, value_as_list, value_as_object};

/// Write a value to the underlying json representation.
pub trait ValueEncoder {
    fn encode(self, v: ValueSer);
}

pub(crate) fn write_int(x: impl itoa::Integer, b: &mut Vec<u8>) {
    b.extend_from_slice(itoa::Buffer::new().format(x).as_bytes());
}

pub(crate) fn write_float(x: impl ryu::Float, b: &mut Vec<u8>) {
    b.extend_from_slice(ryu::Buffer::new().format(x).as_bytes());
}

impl<T: Copy + ValueEncoder> ValueEncoder for &T {
    #[inline]
    fn encode(self, v: ValueSer) {
        T::encode(*self, v);
    }
}

impl ValueEncoder for &str {
    #[inline]
    fn encode(self, v: ValueSer) {
        format_escaped_str(v.buf, self);
        v.finish();
    }
}

impl ValueEncoder for fmt::Arguments<'_> {
    #[inline]
    fn encode(self, v: ValueSer) {
        if let Some(s) = self.as_str() {
            format_escaped_str(v.buf, s);
        } else {
            format_escaped_fmt(v.buf, self);
        }
        v.finish();
    }
}

macro_rules! int {
    [$($t:ty),*] => {
        $(
            impl ValueEncoder for $t {
                #[inline]
                fn encode(self, v: ValueSer) {
                    write_int(self, v.buf);
                    v.finish();
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
                #[inline]
                fn encode(self, v: ValueSer) {
                    write_float(self, v.buf);
                    v.finish();
                }
            }
        )*
    };
}

float![f32, f64];

impl ValueEncoder for bool {
    #[inline]
    fn encode(self, v: ValueSer) {
        v.write_raw_json(if self { b"true" } else { b"false" });
    }
}

impl<T: ValueEncoder> ValueEncoder for Option<T> {
    #[inline]
    fn encode(self, v: ValueSer) {
        match self {
            Some(value) => value.encode(v),
            None => Null.encode(v),
        }
    }
}

impl KeyEncoder for &str {
    #[inline]
    fn write_key<'a>(self, obj: &'a mut ObjectSer) -> ValueSer<'a> {
        let obj = &mut *obj;
        obj.entry_inner(|b| format_escaped_str(b, self))
    }
}

impl KeyEncoder for fmt::Arguments<'_> {
    #[inline]
    fn write_key<'a>(self, obj: &'a mut ObjectSer) -> ValueSer<'a> {
        if let Some(key) = self.as_str() {
            obj.entry_inner(|b| format_escaped_str(b, key))
        } else {
            obj.entry_inner(|b| format_escaped_fmt(b, self))
        }
    }
}

/// Represents the JSON null value.
pub struct Null;

impl ValueEncoder for Null {
    #[inline]
    fn encode(self, v: ValueSer) {
        v.write_raw_json(b"null");
    }
}

impl<T: ValueEncoder> ValueEncoder for Vec<T> {
    #[inline]
    fn encode(self, v: ValueSer) {
        value_as_list!(|v| {
            for t in self {
                v.entry().value(t);
            }
        })
    }
}

impl<T: Copy + ValueEncoder> ValueEncoder for &[T] {
    #[inline]
    fn encode(self, v: ValueSer) {
        value_as_list!(|v| {
            for t in self {
                v.entry().value(t);
            }
        })
    }
}

impl<K: KeyEncoder, V: ValueEncoder, S> ValueEncoder for HashMap<K, V, S> {
    #[inline]
    fn encode(self, o: ValueSer) {
        value_as_object!(|o| {
            for (k, v) in self {
                o.entry(k, v);
            }
        })
    }
}

impl<K: KeyEncoder, V: ValueEncoder> ValueEncoder for BTreeMap<K, V> {
    #[inline]
    fn encode(self, o: ValueSer) {
        value_as_object!(|o| {
            for (k, v) in self {
                o.entry(k, v);
            }
        })
    }
}
