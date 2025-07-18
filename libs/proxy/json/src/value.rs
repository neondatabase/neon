use std::collections::{BTreeMap, HashMap};

use crate::{ValueSer, value_as_list, value_as_object};

/// Marker trait for values that are valid keys
pub trait KeyEncoder: ValueEncoder {}

/// Write a value to the underlying json representation.
pub trait ValueEncoder: Sized {
    fn encode(self, v: ValueSer<'_>);
}

pub(crate) fn write_int(x: impl itoa::Integer, b: &mut Vec<u8>) {
    b.extend_from_slice(itoa::Buffer::new().format(x).as_bytes());
}

pub(crate) fn write_float(x: impl ryu::Float, b: &mut Vec<u8>) {
    b.extend_from_slice(ryu::Buffer::new().format(x).as_bytes());
}

impl<T: Copy + ValueEncoder> ValueEncoder for &T {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        T::encode(*self, v);
    }
}

impl KeyEncoder for String {}
impl ValueEncoder for String {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        self.as_str().encode(v);
    }
}

macro_rules! int {
    [$($t:ty),*] => {
        $(
            impl ValueEncoder for $t {
                #[inline]
                fn encode(self, v: ValueSer<'_>) {
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
                fn encode(self, v: ValueSer<'_>) {
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
    fn encode(self, v: ValueSer<'_>) {
        v.write_raw_json(if self { b"true" } else { b"false" });
    }
}

impl<T: ValueEncoder> ValueEncoder for Option<T> {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        match self {
            Some(value) => value.encode(v),
            None => Null.encode(v),
        }
    }
}

/// Represents the JSON null value.
pub struct Null;

impl ValueEncoder for Null {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        v.write_raw_json(b"null");
    }
}

impl<T: ValueEncoder> ValueEncoder for Vec<T> {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        value_as_list!(|v| {
            for t in self {
                v.entry().value(t);
            }
        });
    }
}

impl<T: Copy + ValueEncoder> ValueEncoder for &[T] {
    #[inline]
    fn encode(self, v: ValueSer<'_>) {
        value_as_list!(|v| {
            for t in self {
                v.entry().value(t);
            }
        });
    }
}

impl<K: KeyEncoder, V: ValueEncoder, S> ValueEncoder for HashMap<K, V, S> {
    #[inline]
    fn encode(self, o: ValueSer<'_>) {
        value_as_object!(|o| {
            for (k, v) in self {
                o.entry(k, v);
            }
        });
    }
}

impl<K: KeyEncoder, V: ValueEncoder> ValueEncoder for BTreeMap<K, V> {
    #[inline]
    fn encode(self, o: ValueSer<'_>) {
        value_as_object!(|o| {
            for (k, v) in self {
                o.entry(k, v);
            }
        });
    }
}
