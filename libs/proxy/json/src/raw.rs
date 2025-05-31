use core::fmt;

use crate::{
    KeyEncoder, ObjectSer, ValueEncoder, ValueSer,
    str::{format_escaped_fmt, format_escaped_str},
};

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

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn new_str(s: &str) -> Box<Self> {
        let mut v = vec![];
        v.reserve_exact(2 + s.len());
        format_escaped_str(&mut v, s);
        Self::boxed_new_unchecked(v.into_boxed_slice())
    }

    #[inline]
    pub fn new_str_fmt(s: fmt::Arguments) -> Box<Self> {
        if let Some(s) = s.as_str() {
            return Self::new_str(s);
        }

        let mut v = vec![];
        format_escaped_fmt(&mut v, s);
        Self::boxed_new_unchecked(v.into_boxed_slice())
    }

    #[inline]
    pub fn new_int(x: impl itoa::Integer) -> Box<Self> {
        Self::boxed_new_unchecked(itoa::Buffer::new().format(x).as_bytes().into())
    }

    #[inline]
    pub fn new_float(x: impl ryu::Float) -> Box<Self> {
        Self::boxed_new_unchecked(ryu::Buffer::new().format(x).as_bytes().into())
    }
}

pub(crate) fn write_int(x: impl itoa::Integer, b: &mut Vec<u8>) {
    b.extend_from_slice(itoa::Buffer::new().format(x).as_bytes());
}

pub(crate) fn write_float(x: impl ryu::Float, b: &mut Vec<u8>) {
    b.extend_from_slice(ryu::Buffer::new().format(x).as_bytes());
}

impl ValueEncoder for &RawValue {
    #[inline]
    fn encode(self, v: ValueSer) {
        v.buf.extend_from_slice(self.as_bytes());
        v.finish();
    }
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
        v.value(if self {
            RawValue::TRUE
        } else {
            RawValue::FALSE
        });
    }
}

impl<T: ValueEncoder> ValueEncoder for Option<T> {
    #[inline]
    fn encode(self, v: ValueSer) {
        match self {
            Some(value) => value.encode(v),
            None => RawValue::NULL.encode(v),
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
